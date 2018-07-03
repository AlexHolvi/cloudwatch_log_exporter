#!/usr/bin/env python3
import boto3
import time
from datetime import datetime, timedelta
import os
from typing import List, NewType, Dict, Optional, Tuple

SnsResponse = NewType("SnsResponse", str)
LogLine = NewType("LogLine", str)
LogGroup = NewType("LogGroup", dict)
LogGroupName = NewType("LogGroupName", str)
NextToken = NewType("NextToken", str)


def make_batches(l: List, n: int):
    """Yield successive n-sized chunks from l."""
    return tuple(l[i:i + n] for i in range(0, len(l), n))


def create_aws_client(resource: str= "logs", **kwargs) -> boto3.client:
    session = boto3.Session(**kwargs)
    return session.client(resource, region_name=os.environ.get("LOGS_REGION", "us-west-1"))


def publish_to_sns(message: str, c: boto3.client) -> SnsResponse:
        return SnsResponse(c.publish(
            TopicArn=os.environ.get("LOGS_SNS_TOPIC", ""),
            Message=message,
            Subject='CloudWatch Logs Export Results'
        ))


def get_export_status_code(c: boto3.client, task_id: str) -> str:
    # Todo: create type with proper validation
    return c.describe_export_tasks(taskId=task_id)["exportTasks"][0]["status"]["code"]


def get_group_name_and_token(lg: LogGroup) -> Tuple[List[LogGroupName], Optional[NextToken]]:
    try:
        next_token = NextToken(lg["NextToken"])
    except KeyError:
        next_token = None
    return [log_group(lg["logGroupName"]) for log_group in lg["logGroups"]], next_token


def maybe_export_log_group(lc: boto3.client, start_time, end_time, lgn: LogGroupName, retries: int = 10, interval: int = 10) -> str:
    res = SnsResponse(lc.create_export_task(
        taskName=lgn,
        logGroupName=lgn,
        fromTime=start_time,
        to=end_time,
        destination=os.environ.get("LOGS_BUCKET", "cloudwatch-logs-coldstorage"),
        destinationPrefix=f"cloudwatchlogs_{lgn}/{start_time.isoformat()}"
    ))

    attempts = 0
    while get_export_status_code(c=lc, task_id=res["taskId"]) != "COMPLETED":
        if attempts > retries:
            return

        attempts += 1
        time.sleep(interval)

    return f"{res} export status: {get_export_status_code(c=lc, task_id=res['taskId'])}\n"


def get_logs(logs_from_hours_ago: int, cloudwatch_staleness_slo=12) -> SnsResponse:
    """
    :param cloudwatch_staleness_slo: Cloudwatch logs availability has a bounded staleness for 12 hours (2018)
    :param logs_from_hours_ago: how many hours into the past to go when starting logging
    :return: SnsResponse
    """
    current_time = datetime.utcnow()
    log_start_time = current_time - timedelta(hours=logs_from_hours_ago)
    log_end_time = current_time - timedelta(hours=cloudwatch_staleness_slo)

    log_client = create_aws_client(resource="logs", profile_name="localadmin")
    sns_client = create_aws_client(resource="sns", profile_name="localadmin")

    initial_log_group_group_names, last_token = get_group_name_and_token(
            log_client.describe_log_groups())

    results_report = "\n".join([maybe_export_log_group(lc=log_client, start_time=log_start_time, end_time=log_end_time, lgn=x) for x in initial_log_group_group_names])

    # in case we have more than 50 results, aws will send a nextToken and there is
    # see https://boto3.readthedocs.io/en/latest/reference/services/logs.html#CloudWatchLogs.Client.describe_log_groups
    while last_token:
        log_group_group_name, last_token = get_group_name_and_token(
            log_client.describe_log_groups(nextToken=last_token))

        progress_message = f"{time.strftime('%Y-%d-%m %H:%M:%S')}: {log_group_group_name}"

        print(progress_message)
        results_report += f"\n{progress_message}"

        results_report += "\n".join([maybe_export_log_group(lc=log_client, start_time=log_start_time, end_time=log_end_time, lgn=x) for x in initial_log_group_group_names])

    return publish_to_sns(message=results_report, c=sns_client)


if __name__ == '__main__':
    result = get_logs(logs_from_hours_ago=36)
    print(result)
