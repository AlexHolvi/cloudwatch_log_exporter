#!/usr/bin/env python3
import boto3
import time
from datetime import datetime, timedelta
import os
from typing import List, Optional, Tuple
from data_types import *


def make_batches(l: List, n: int):
    """Yield successive n-sized chunks from l."""
    return tuple(l[i:i + n] for i in range(0, len(l), n))


def create_aws_client(resource: str= "logs", **kwargs) -> boto3.client:
    session = boto3.Session(**kwargs)
    return session.client(resource, region_name=os.environ.get("LOGS_REGION", "eu-west-1"))


def publish_to_sns(message: str, c: boto3.client, subject: str = 'CloudWatch Logs Export Results') -> SnsResponse:
        return SnsResponse(c.publish(
            TopicArn=os.environ.get("LOGS_SNS_TOPIC", "arn:aws:sns:eu-west-1:973635090400:CWLogsUpload"),
            Message=message,
            Subject=subject
        ))


def get_export_status_code(c: boto3.client, task_id: str) -> str:
    # Todo: create type with proper validation
    return c.describe_export_tasks(taskId=task_id)["exportTasks"][0]["status"]["code"]


def get_group_name_and_token(lg: LogGroup) -> Tuple[List[LogGroupName], Optional[NextToken]]:
    try:
        next_token = NextToken(lg["NextToken"])
    except KeyError:
        next_token = None
    return [LogGroupName(lgn["logGroupName"]) for lgn in lg["logGroups"]], next_token


def maybe_export_log_group(lc: boto3.client, start_time: AwsTime, end_time: AwsTime, prefix_time: HumanTime,
                           lgn: LogGroupName, retries: int = 10, interval: int = 10) -> Optional[str]:

    res = SnsResponse(lc.create_export_task(
        taskName=lgn,
        logGroupName=lgn,
        fromTime=start_time,
        to=end_time,
        destination=os.environ.get("LOGS_BUCKET", "cloudwatch-logs-coldstorage"),
        destinationPrefix=f"cloudwatchlogs_{lgn}/{prefix_time}"
    ))

    attempts = 0
    while get_export_status_code(c=lc, task_id=res["taskId"]) != "COMPLETED":
        if attempts > retries:
            return None

        attempts += 1
        time.sleep(interval)

    return f"{res} export status: {get_export_status_code(c=lc, task_id=res['taskId'])}\n"


def get_logs(logs_from_hours_ago: int = 36, cloudwatch_staleness_slo: int = 12) -> SnsResponse:
    """
    :param cloudwatch_staleness_slo: Cloudwatch logs availability has a bounded staleness for 12 hours (2018)
    :param logs_from_hours_ago: how many hours into the past to go when starting logging
    :return: SnsResponse
    """
    date_format = '%Y-%d-%m %H:%M:%S'
    current_time = datetime.utcnow()
    # Human-readable and AWS timestamp formats (API does not work with timestamps and requires an int)
    log_start_time = (current_time - timedelta(hours=logs_from_hours_ago))
    log_start_time_hr = HumanTime(log_start_time.strftime(date_format))
    log_start_time_ts = AwsTime(int(log_start_time.timestamp() * 1000))
    log_end_time = (current_time - timedelta(hours=cloudwatch_staleness_slo))
    log_end_time_hr = HumanTime(log_end_time.strftime(date_format))
    log_end_time_ts = AwsTime(int(log_end_time.timestamp() * 1000))

    print(f"{datetime.utcnow()} creating client connection")
    log_client = create_aws_client(resource="logs", profile_name="localadmin")
    sns_client = create_aws_client(resource="sns", profile_name="localadmin")
    # sess = boto3.Session(profile_name="localadmin")
    # sns_client = sess.client('sns')

    print(f"{datetime.utcnow()} getting log groups")
    print(f"received time start: {log_start_time_hr}, end: {log_end_time_hr}")
    initial = log_client.describe_log_groups(limit=50)
    print(initial)
    initial_log_group_group_names, last_token = get_group_name_and_token(
            initial)

    print(f"got {initial_log_group_group_names}\n last token: {last_token}")

    # Todo: stop lying that this is a List[str] and handle exceptions in maybe
    results_report = "\n".join([maybe_export_log_group(
        lc=log_client,
        start_time=log_start_time_ts,
        end_time=log_end_time_ts,
        prefix_time=log_start_time_hr,
        lgn=x)
        for x in initial_log_group_group_names])

    # in case we have more than 50 results, aws will send a nextToken and there is
    # see https://boto3.readthedocs.io/en/latest/reference/services/logs.html#CloudWatchLogs.Client.describe_log_groups
    while last_token:
        print(f"{datetime.utcnow()} last token was: {last_token}")
        log_group_group_name, last_token = get_group_name_and_token(
            log_client.describe_log_groups(nextToken=last_token))
        print(f"{datetime.utcnow()} last token changed to: {last_token}")

        progress_message = f"{time.strftime(date_format)}: {log_group_group_name}"

        print(f"{datetime.utcnow()} {progress_message}")

        results_report += f"\n{progress_message}"
        results_report += "\n".join([maybe_export_log_group(
            lc=log_client,
            start_time=log_start_time_ts,
            end_time=log_end_time_ts,
            prefix_time=log_start_time_hr,
            lgn=x)
            for x in initial_log_group_group_names])

    return publish_to_sns(message=results_report, c=sns_client)


if __name__ == '__main__':
    result = get_logs(logs_from_hours_ago=36)
    print(result)
