from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.trigger_rule import TriggerRule
from utils import datafeed_py, handle_task_exception, reduce_dag_run_exceptions
from datetime import datetime
import os
import json


with DAG(
        dag_id=f"airflow-template",
        schedule=None,
        start_date=datetime(2024,5,5),
        catchup=False,
        tags=["empty-template"],
        default_args=dict(owner='airflow')
        ) as dag:
    with open(f"{os.path.dirname(__file__)}/configuration/conf.dev.json", "r") as f:
        conf = json.loads(f.read())

    @task.external_python(
        task_id="scanner",
        python=datafeed_py(__file__),
        on_failure_callback=handle_task_exception,
    )
    def scanner(configuration):
        from src import DataFeed
        from src.steps.scanner.step import ScannerStep

        d = DataFeed.create(configuration)
        return d.steps[ScannerStep]()

    @task.external_python(
        task_id="validator",
        python=datafeed_py(__file__),
        on_failure_callback=handle_task_exception,
    )
    def validator(configuration, found_files):
        from src import DataFeed
        from src.steps.validator.step import ValidatorStep
        from src.steps.validator.conditions.name import (
            LongNameCondition,
            SpecialCharactersCondition,
        )

        d = DataFeed.create(configuration)
        return d.steps[ValidatorStep](
            conditions=[
                LongNameCondition(max_length=100),
                SpecialCharactersCondition(),
            ],
            metadata_list=found_files,
        )

    @task.external_python(
        task_id="handler",
        python=datafeed_py(__file__),
        on_failure_callback=handle_task_exception,
    )
    def handler(configuration, validation_results):
        from src import DataFeed
        from src.steps.handler.step import HandlerStep

        d = DataFeed.create(configuration)
        return d.steps[HandlerStep](validated_metadata=validation_results)

    @task.external_python(
        task_id="reader",
        python=datafeed_py(__file__),
        on_failure_callback=handle_task_exception,
    )
    def reader(configuration, handled_files):
        from src import DataFeed
        from src.steps.reader.step import ReaderStep

        archive_files, rejected_files = handled_files
        d = DataFeed.create(configuration)
        return d.steps[ReaderStep](metadata_list=archive_files)

    @task.external_python(
        task_id="uploader",
        python=datafeed_py(__file__),
        on_failure_callback=handle_task_exception,
    )
    def uploader(configuration, files):
        from src import DataFeed
        from src.steps.uploader.step import UploaderStep

        d = DataFeed.create(configuration)
        d.steps[UploaderStep](files=files)

    @task.external_python(
        task_id="notify_warning",
        python=datafeed_py(__file__),
        on_failure_callback=handle_task_exception,
    )
    def notify_warning(configuration, validation_results):
        from src import DataFeed
        from src.steps.notifier.step import NotifierStep
        from src.steps.notifier.notification import NotificationType

        d = DataFeed.create(configuration)
        d.steps[NotifierStep](
            notification_type=NotificationType.Warning,
            metadata_conf=d.metadata,
            filemetadata_validated=validation_results,
        )

    @task.external_python(
        task_id="notify_information",
        python=datafeed_py(__file__),
        on_failure_callback=handle_task_exception,
    )
    def notify_information(configuration, validation_results):
        from src import DataFeed
        from src.steps.notifier.step import NotifierStep
        from src.steps.notifier.notification import NotificationType

        d = DataFeed.create(configuration)
        d.steps[NotifierStep](
            notification_type=NotificationType.Warning,
            metadata_conf=d.metadata,
            filemetadata_validated=validation_results,
        )

    @task.external_python(
        task_id="notify_success",
        python=datafeed_py(__file__),
        on_failure_callback=handle_task_exception,
    )
    def notify_success(configuration, files_uploaded):
        from src import DataFeed
        from src.steps.notifier.step import NotifierStep
        from src.steps.notifier.notification import NotificationType

        d = DataFeed.create(configuration)
        d.steps[NotifierStep](
            notification_type=NotificationType.Success,
            metadata_conf=d.metadata,
            file_list=files_uploaded,
        )

    @task.short_circuit(task_id="are_files", on_failure_callback=handle_task_exception)
    def are_files(found_files):
        return any(found_files)

    @task.branch(task_id="are_valid", on_failure_callback=handle_task_exception)
    def are_valid(validation_results):
        for _, value in validation_results.items():
            if len(value) > 0:
                return "task_group_feed.notify_warning"
        return "task_group_feed.notify_success"

    @task.external_python(task_id="removar", python=datafeed_py(__file__), on_failure_callback=handle_task_exception)
    def removar(configuration, filemetadatas):
        from src import DataFeed
        from src.steps.removar.step import RemovarStep
        (archive_metadatas, rejected_metadatas) = filemetadatas
        
        d = DataFeed.create(configuration)
        d.steps[RemovarStep](metadata_list=archive_metadatas)

    @task(task_id="gather_exceptions", trigger_rule=TriggerRule.ONE_FAILED)
    def gather_exceptions(**context):
        return reduce_dag_run_exceptions(dag, context)

    @task.external_python(task_id="notify_error", python=datafeed_py(__file__))
    def notify_error(configuration, task_exceptions):
        from src import DataFeed
        from src.steps.notifier.step import NotifierStep
        from src.steps.notifier.notification import NotificationType

        d = DataFeed.create(configuration)
        d.steps[NotifierStep](
            notification_type=NotificationType.Error,
            metadata_conf=d.metadata,
            exceptions=task_exceptions,
        )

    @task_group(group_id="task_group_feed")
    def task_group_feed():
        scan = scanner(conf)
        are_files_choice = are_files(scan)
        validate = validator(conf, scan)
        handle = handler(conf, validate)
        read = reader(conf, handle)
        upload = uploader(conf, read)
        remove = removar(conf, handle)
        are_all_valid_choice = are_valid(validate)

        notification_warning = notify_warning(conf, validate)
        notification_success = notify_success(conf, read)

        are_files_choice >> validate
        are_all_valid_choice >> notification_warning
        are_all_valid_choice >> notification_success
        upload >> remove >> notification_success
        handle >> notification_warning

        upload >> remove >> notification_warning

    @task_group(group_id="task_group_exception")
    def task_group_exceptions():
        notify_error(conf, gather_exceptions())

    task_group_feed() >> task_group_exceptions()
