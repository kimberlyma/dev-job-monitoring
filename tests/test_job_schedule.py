import pytest
from databricks.sdk.service.jobs import CronSchedule, JobSettings, PauseStatus

from job_monitoring.job_monitoring import out_of_policy


@pytest.fixture()
def out_of_policy_job():
    schedule = CronSchedule(
        quartz_cron_expression="54 30 11 ? * Sun",
        timezone_id="UTC",
        pause_status=PauseStatus.UNPAUSED,
    )
    tags = {"team" : "data-science"}
    return JobSettings(schedule=schedule, tags=tags)

@pytest.fixture()
def in_policy_paused_job():
    schedule = CronSchedule(
        quartz_cron_expression="54 30 11 ? * Sun",
        timezone_id="UTC",
        pause_status=PauseStatus.PAUSED,
    )
    tags = {"team" : "data-science"}
    return JobSettings(schedule=schedule, tags=tags)

@pytest.fixture()
def in_policy_tagged_job():
    schedule = CronSchedule(
        quartz_cron_expression="54 30 11 ? * Sun",
        timezone_id="UTC",
        pause_status=PauseStatus.UNPAUSED,
    )
    tags = {"keep_alive" : "true"}
    return JobSettings(schedule=schedule, tags=tags)

def test_out_of_policy_job(out_of_policy_job):
    assert(out_of_policy(out_of_policy_job) is True)

def test_in_policy_paused_job(in_policy_paused_job):
    assert(out_of_policy(in_policy_paused_job) is False)

def test_in_policy_tagged_job(in_policy_tagged_job):
    assert(out_of_policy(in_policy_tagged_job) is False)