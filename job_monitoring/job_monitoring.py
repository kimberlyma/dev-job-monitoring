import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import CronSchedule, JobSettings, PauseStatus

# Initialize WorkspaceClient
w = WorkspaceClient()


def update_new_settings(job_id, quarts_cron_expression, timezone_id):
    """Update out of policy job schedules to be paused"""
    new_schedule = CronSchedule(
        quartz_cron_expression=quarts_cron_expression,
        timezone_id=timezone_id,
        pause_status=PauseStatus.PAUSED,
    )
    new_settings = JobSettings(schedule=new_schedule)

    logging.info(f"Job id: {job_id}, new_settings: {new_settings}")
    w.jobs.update(job_id, new_settings=new_settings)


def out_of_policy(job_settings: JobSettings):
    """Check if a job is out of policy.
    out of policy - a scheduled job is unpaused and is not tagged as keep_alive
    Return true if out of policy, false if in policy
    """

    tagged = bool(job_settings.tags)
    proper_tags = tagged and "keep_alive" in job_settings.tags
    paused = job_settings.schedule.pause_status is PauseStatus.PAUSED

    return not paused and not proper_tags

all_jobs = w.jobs.list()
for job in all_jobs:
    job_id = job.job_id
    if job.settings.schedule and out_of_policy(job.settings):
        schedule = job.settings.schedule

        logging.info(
            f"Job name: {job.settings.name}, Job id: {job_id}, creator: {job.creator_user_name}, schedule: {schedule}"
        )
