from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

all_jobs = w.jobs.list()

for job in all_jobs:
    job_id = job.job_id