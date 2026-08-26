"""Direct examples of the YARN and Spark monitoring REST APIs."""

import json
from datetime import datetime, timedelta

import httpx


YARN_URL = "http://localhost:8088"


def get_yarn_applications(
    states: str | None = None,
    application_types: str | None = None,
) -> dict:
    """Get the applications known to the YARN ResourceManager."""
    params = {}
    if states is not None:
        params["states"] = states
    if application_types is not None:
        params["applicationTypes"] = application_types

    response = httpx.get(f"{YARN_URL}/ws/v1/cluster/apps", params=params)
    response.raise_for_status()
    return response.json()


def get_yarn_application(application_id: str) -> dict:
    """Get one YARN application and its ApplicationMaster information."""
    response = httpx.get(f"{YARN_URL}/ws/v1/cluster/apps/{application_id}")
    response.raise_for_status()
    return response.json()


def get_yarn_application_attempts(application_id: str) -> dict:
    """Get every ApplicationMaster attempt for one YARN application."""
    response = httpx.get(
        f"{YARN_URL}/ws/v1/cluster/apps/{application_id}/appattempts"
    )
    response.raise_for_status()
    return response.json()


def get_spark_applications(application_id: str) -> list[dict]:
    """Get the Spark applications exposed by a YARN ApplicationMaster."""
    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications"
    )
    response.raise_for_status()
    return response.json()


def get_spark_application(application_id: str) -> dict:
    """Get the Spark application associated with a YARN application."""
    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/{application_id}"
    )
    response.raise_for_status()
    return response.json()


def get_jobs(application_id: str, status: str | None = None) -> list[dict]:
    """Get every Spark job, optionally filtered by job status."""
    params = {}
    if status is not None:
        params["status"] = status

    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/"
        f"{application_id}/jobs",
        params=params,
    )
    response.raise_for_status()
    return response.json()


def get_job(application_id: str, job_id: int) -> dict:
    """Get one Spark job. Its stageIds field identifies the job's stages."""
    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/"
        f"{application_id}/jobs/{job_id}"
    )
    response.raise_for_status()
    return response.json()


def get_stages(application_id: str, status: str | None = None) -> list[dict]:
    """Get every Spark stage, optionally filtered by stage status."""
    params = {}
    if status is not None:
        params["status"] = status

    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/"
        f"{application_id}/stages",
        params=params,
    )
    response.raise_for_status()
    return response.json()


def get_stage_attempts(application_id: str, stage_id: int) -> list[dict]:
    """Get every attempt of one stage, including failed attempts."""
    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/"
        f"{application_id}/stages/{stage_id}"
    )
    response.raise_for_status()
    return response.json()


def get_stage_attempt(
    application_id: str,
    stage_id: int,
    stage_attempt_id: int,
) -> dict:
    """Get the complete details and metrics for one stage attempt."""
    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/"
        f"{application_id}/stages/{stage_id}/{stage_attempt_id}"
    )
    response.raise_for_status()
    return response.json()


def get_tasks(
    application_id: str,
    stage_id: int,
    stage_attempt_id: int,
    offset: int = 0,
    length: int = 1000,
    sort_by: str = "ID",
) -> list[dict]:
    """Get one page of task attempts for a stage attempt."""
    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/"
        f"{application_id}/stages/{stage_id}/{stage_attempt_id}/taskList",
        params={"offset": offset, "length": length, "sortBy": sort_by},
    )
    response.raise_for_status()
    return response.json()


def get_task_summary(
    application_id: str,
    stage_id: int,
    stage_attempt_id: int,
    quantiles: str = "0.5,0.75,0.9,0.99",
) -> dict:
    """Get task metric quantiles for one stage attempt."""
    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/"
        f"{application_id}/stages/{stage_id}/{stage_attempt_id}/taskSummary",
        params={"quantiles": quantiles},
    )
    response.raise_for_status()
    return response.json()


def get_active_executors(application_id: str) -> list[dict]:
    """Get the executors currently registered with the Spark application."""
    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/"
        f"{application_id}/executors"
    )
    response.raise_for_status()
    return response.json()


def get_all_executors(application_id: str) -> list[dict]:
    """Get active and removed executors, including removal reasons."""
    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/"
        f"{application_id}/allexecutors"
    )
    response.raise_for_status()
    return response.json()


def get_sql_executions(application_id: str) -> list[dict]:
    """Get SQL executions and the job IDs associated with each query."""
    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/"
        f"{application_id}/sql"
    )
    response.raise_for_status()
    return response.json()


def get_sql_execution(application_id: str, sql_execution_id: int) -> dict:
    """Get the plan, metrics, jobs, and stages for one SQL execution."""
    response = httpx.get(
        f"{YARN_URL}/proxy/{application_id}/api/v1/applications/"
        f"{application_id}/sql/{sql_execution_id}"
    )
    response.raise_for_status()
    return response.json()


# Set these two values. Spark Connect wraps your tag in a longer internal tag,
# so the match below checks for the exact "_Tag_<your tag>" suffix.
application_id = "application_1787533953140_0001"
tag = "script=customer_rollup.py"


# Find every job created by an operation carrying this tag.
all_jobs = get_jobs(application_id)
jobs = [
    job
    for job in all_jobs
    if any(
        job_tag == tag or job_tag.endswith(f"_Tag_{tag}")
        for job_tag in job.get("jobTags", [])
    )
]

if not jobs:
    available_tags = sorted(
        {
            job_tag
            for job in all_jobs
            for job_tag in job.get("jobTags", [])
            if "_Tag_" in job_tag
        }
    )
    raise RuntimeError(
        f"No jobs in {application_id} matched {tag!r}. "
        f"Available tags: {available_tags}"
    )


# Find SQL executions whose job lists overlap the tagged jobs.
job_ids = {job["jobId"] for job in jobs}
sql_executions = [
    sql
    for sql in get_sql_executions(application_id)
    if job_ids
    & set(
        sql.get("runningJobIds", [])
        + sql.get("successJobIds", [])
        + sql.get("failedJobIds", [])
    )
]


# A stage can appear in more than one adaptive-query job. Record the jobs that
# reference it, but only download and count that stage's tasks once.
stage_job_ids = {}
for job in jobs:
    for stage_id in job.get("stageIds", []):
        stage_job_ids.setdefault(stage_id, []).append(job["jobId"])


stage_reports = []
all_tagged_tasks = []

for stage_id, containing_job_ids in sorted(stage_job_ids.items()):
    for stage_attempt in get_stage_attempts(application_id, stage_id):
        stage_attempt_id = stage_attempt["attemptId"]

        # The API is paginated. Keep asking for pages until one is short.
        tasks = []
        while True:
            task_page = get_tasks(
                application_id,
                stage_id,
                stage_attempt_id,
                offset=len(tasks),
                length=1000,
            )
            tasks.extend(task_page)
            if len(task_page) < 1000:
                break

        for task in tasks:
            task["stageId"] = stage_id
            task["stageAttemptId"] = stage_attempt_id
        all_tagged_tasks.extend(tasks)

        task_durations = sorted(task.get("duration", 0) or 0 for task in tasks)
        task_bytes = sorted(
            (task.get("taskMetrics", {}).get("inputMetrics", {}).get("bytesRead", 0) or 0)
            + (
                task.get("taskMetrics", {})
                .get("shuffleReadMetrics", {})
                .get("remoteBytesRead", 0)
                or 0
            )
            + (
                task.get("taskMetrics", {})
                .get("shuffleReadMetrics", {})
                .get("localBytesRead", 0)
                or 0
            )
            for task in tasks
        )

        task_count = len(tasks)
        median_index = (task_count - 1) // 2 if task_count else 0
        p95_index = round(0.95 * (task_count - 1)) if task_count else 0
        median_duration = task_durations[median_index] if task_count else 0
        p95_duration = task_durations[p95_index] if task_count else 0
        median_bytes = task_bytes[median_index] if task_count else 0
        p95_bytes = task_bytes[p95_index] if task_count else 0

        executor_run_time_ms = sum(
            task.get("taskMetrics", {}).get("executorRunTime", 0) or 0
            for task in tasks
        )
        executor_cpu_time_ms = sum(
            (task.get("taskMetrics", {}).get("executorCpuTime", 0) or 0) / 1_000_000
            for task in tasks
        )
        jvm_gc_time_ms = sum(
            task.get("taskMetrics", {}).get("jvmGcTime", 0) or 0
            for task in tasks
        )
        scheduler_delay_ms = sum(task.get("schedulerDelay", 0) or 0 for task in tasks)
        total_task_duration_ms = sum(task_durations)
        shuffle_fetch_wait_ms = sum(
            task.get("taskMetrics", {})
            .get("shuffleReadMetrics", {})
            .get("fetchWaitTime", 0)
            or 0
            for task in tasks
        )
        input_bytes = sum(
            task.get("taskMetrics", {}).get("inputMetrics", {}).get("bytesRead", 0)
            or 0
            for task in tasks
        )
        shuffle_read_bytes = sum(
            (
                task.get("taskMetrics", {})
                .get("shuffleReadMetrics", {})
                .get("remoteBytesRead", 0)
                or 0
            )
            + (
                task.get("taskMetrics", {})
                .get("shuffleReadMetrics", {})
                .get("localBytesRead", 0)
                or 0
            )
            for task in tasks
        )
        shuffle_write_bytes = sum(
            task.get("taskMetrics", {})
            .get("shuffleWriteMetrics", {})
            .get("bytesWritten", 0)
            or 0
            for task in tasks
        )
        memory_spilled_bytes = sum(
            task.get("taskMetrics", {}).get("memoryBytesSpilled", 0) or 0
            for task in tasks
        )
        disk_spilled_bytes = sum(
            task.get("taskMetrics", {}).get("diskBytesSpilled", 0) or 0
            for task in tasks
        )
        processed_bytes = input_bytes + shuffle_read_bytes
        failed_tasks = sum(
            task.get("status") not in {"SUCCESS", "RUNNING"} for task in tasks
        )

        stage_reports.append(
            {
                "stageId": stage_id,
                "stageAttemptId": stage_attempt_id,
                "jobIds": containing_job_ids,
                "name": stage_attempt.get("name"),
                "status": stage_attempt.get("status"),
                "tasks": task_count,
                "failedOrKilledTaskAttempts": failed_tasks,
                "failureRate": round(failed_tasks / task_count, 4) if task_count else 0,
                "executorRunTimeMs": executor_run_time_ms,
                "cpuEfficiency": (
                    round(executor_cpu_time_ms / executor_run_time_ms, 4)
                    if executor_run_time_ms
                    else None
                ),
                "gcFraction": (
                    round(jvm_gc_time_ms / executor_run_time_ms, 4)
                    if executor_run_time_ms
                    else None
                ),
                "schedulerDelayFraction": (
                    round(scheduler_delay_ms / total_task_duration_ms, 4)
                    if total_task_duration_ms
                    else None
                ),
                "shuffleFetchWaitFraction": (
                    round(shuffle_fetch_wait_ms / executor_run_time_ms, 4)
                    if executor_run_time_ms
                    else None
                ),
                "inputBytes": input_bytes,
                "shuffleReadBytes": shuffle_read_bytes,
                "shuffleWriteBytes": shuffle_write_bytes,
                "memorySpilledBytes": memory_spilled_bytes,
                "diskSpilledBytes": disk_spilled_bytes,
                "spillRatio": (
                    round((memory_spilled_bytes + disk_spilled_bytes) / processed_bytes, 4)
                    if processed_bytes
                    else None
                ),
                "shuffleAmplification": (
                    round((shuffle_read_bytes + shuffle_write_bytes) / input_bytes, 4)
                    if input_bytes
                    else None
                ),
                "processedMiBPerCpuSecond": (
                    round(processed_bytes / 1_048_576 / (executor_cpu_time_ms / 1000), 4)
                    if executor_cpu_time_ms
                    else None
                ),
                "medianTaskDurationMs": median_duration,
                "p95TaskDurationMs": p95_duration,
                "maxTaskDurationMs": task_durations[-1] if task_count else None,
                "p95ToMedianTaskDuration": (
                    round(p95_duration / median_duration, 4)
                    if median_duration
                    else None
                ),
                "maxToMedianTaskDuration": (
                    round(task_durations[-1] / median_duration, 4)
                    if median_duration
                    else None
                ),
                "medianTaskBytes": median_bytes,
                "p95TaskBytes": p95_bytes,
                "maxTaskBytes": task_bytes[-1] if task_count else None,
                "p95ToMedianTaskBytes": (
                    round(p95_bytes / median_bytes, 4) if median_bytes else None
                ),
                "maxToMedianTaskBytes": (
                    round(task_bytes[-1] / median_bytes, 4) if median_bytes else None
                ),
            }
        )


# Attribute task work to executors. Executor peakMemoryMetrics are included for
# context, but they are lifetime peaks for the shared Connect application, not
# values isolated to this tag.
executor_details = {
    str(executor["id"]): executor for executor in get_all_executors(application_id)
}
executor_reports = []

# Estimate how fully each stage used the executor cores that actually handled
# its tasks. This is approximate on a shared server because other scripts can
# compete for those same cores during the same wall-clock interval.
for stage_report in stage_reports:
    stage_tasks = [
        task
        for task in all_tagged_tasks
        if task["stageId"] == stage_report["stageId"]
        and task["stageAttemptId"] == stage_report["stageAttemptId"]
    ]
    task_starts = [
        datetime.strptime(task["launchTime"], "%Y-%m-%dT%H:%M:%S.%fGMT")
        for task in stage_tasks
        if task.get("launchTime")
    ]
    task_ends = [
        datetime.strptime(task["launchTime"], "%Y-%m-%dT%H:%M:%S.%fGMT")
        + timedelta(milliseconds=task.get("duration", 0) or 0)
        for task in stage_tasks
        if task.get("launchTime")
    ]
    involved_executor_ids = {
        str(task.get("executorId")) for task in stage_tasks if task.get("executorId")
    }
    available_cores = sum(
        executor_details.get(executor_id, {}).get("totalCores", 0) or 0
        for executor_id in involved_executor_ids
    )
    wall_time_ms = (
        (max(task_ends) - min(task_starts)).total_seconds() * 1000
        if task_starts and task_ends
        else 0
    )
    stage_report["approximateParallelEfficiency"] = (
        round(
            stage_report["executorRunTimeMs"] / (wall_time_ms * available_cores),
            4,
        )
        if wall_time_ms and available_cores
        else None
    )

for executor_id in sorted(
    {str(task.get("executorId")) for task in all_tagged_tasks},
    key=lambda value: (not value.isdigit(), value),
):
    executor_tasks = [
        task for task in all_tagged_tasks if str(task.get("executorId")) == executor_id
    ]
    executor_run_time_ms = sum(
        task.get("taskMetrics", {}).get("executorRunTime", 0) or 0
        for task in executor_tasks
    )
    executor_cpu_time_ms = sum(
        (task.get("taskMetrics", {}).get("executorCpuTime", 0) or 0) / 1_000_000
        for task in executor_tasks
    )
    jvm_gc_time_ms = sum(
        task.get("taskMetrics", {}).get("jvmGcTime", 0) or 0
        for task in executor_tasks
    )
    memory_spilled_bytes = sum(
        task.get("taskMetrics", {}).get("memoryBytesSpilled", 0) or 0
        for task in executor_tasks
    )
    disk_spilled_bytes = sum(
        task.get("taskMetrics", {}).get("diskBytesSpilled", 0) or 0
        for task in executor_tasks
    )
    executor = executor_details.get(executor_id, {})

    executor_reports.append(
        {
            "executorId": executor_id,
            "hostPort": executor.get("hostPort"),
            "isActive": executor.get("isActive"),
            "removedReason": executor.get("removeReason"),
            "taskAttempts": len(executor_tasks),
            "failedOrKilledTaskAttempts": sum(
                task.get("status") not in {"SUCCESS", "RUNNING"}
                for task in executor_tasks
            ),
            "executorRunTimeMs": executor_run_time_ms,
            "workShare": (
                round(
                    executor_run_time_ms
                    / sum(
                        task.get("taskMetrics", {}).get("executorRunTime", 0) or 0
                        for task in all_tagged_tasks
                    ),
                    4,
                )
                if all_tagged_tasks
                and sum(
                    task.get("taskMetrics", {}).get("executorRunTime", 0) or 0
                    for task in all_tagged_tasks
                )
                else None
            ),
            "cpuEfficiency": (
                round(executor_cpu_time_ms / executor_run_time_ms, 4)
                if executor_run_time_ms
                else None
            ),
            "gcFraction": (
                round(jvm_gc_time_ms / executor_run_time_ms, 4)
                if executor_run_time_ms
                else None
            ),
            "memorySpilledBytes": memory_spilled_bytes,
            "diskSpilledBytes": disk_spilled_bytes,
            "maxTaskExecutionMemoryBytes": max(
                (
                    task.get("taskMetrics", {}).get("peakExecutionMemory", 0) or 0
                    for task in executor_tasks
                ),
                default=0,
            ),
            "executorMaxStorageMemoryBytes": executor.get("maxMemory"),
            "applicationLifetimePeakMemoryMetrics": executor.get("peakMemoryMetrics"),
            "executorLogs": executor.get("executorLogs"),
        }
    )


# Rank individual task attempts so failures appear first, followed by tasks
# with the most spill, execution memory, and duration.
task_reports = []
for task in all_tagged_tasks:
    metrics = task.get("taskMetrics", {})
    shuffle_read = metrics.get("shuffleReadMetrics", {})
    task_reports.append(
        {
            "stageId": task["stageId"],
            "stageAttemptId": task["stageAttemptId"],
            "taskId": task.get("taskId"),
            "taskAttempt": task.get("attempt"),
            "executorId": task.get("executorId"),
            "host": task.get("host"),
            "status": task.get("status"),
            "errorMessage": task.get("errorMessage"),
            "durationMs": task.get("duration"),
            "schedulerDelayMs": task.get("schedulerDelay"),
            "executorRunTimeMs": metrics.get("executorRunTime"),
            "executorCpuTimeNs": metrics.get("executorCpuTime"),
            "jvmGcTimeMs": metrics.get("jvmGcTime"),
            "peakExecutionMemoryBytes": metrics.get("peakExecutionMemory"),
            "memorySpilledBytes": metrics.get("memoryBytesSpilled"),
            "diskSpilledBytes": metrics.get("diskBytesSpilled"),
            "inputBytes": metrics.get("inputMetrics", {}).get("bytesRead"),
            "shuffleReadBytes": (shuffle_read.get("remoteBytesRead", 0) or 0)
            + (shuffle_read.get("localBytesRead", 0) or 0),
            "shuffleFetchWaitMs": shuffle_read.get("fetchWaitTime"),
            "shuffleWriteBytes": metrics.get("shuffleWriteMetrics", {}).get(
                "bytesWritten"
            ),
            "executorLogs": task.get("executorLogs"),
        }
    )

task_reports.sort(
    key=lambda task: (
        task["status"] not in {"SUCCESS", "RUNNING"},
        (task["memorySpilledBytes"] or 0) + (task["diskSpilledBytes"] or 0),
        task["peakExecutionMemoryBytes"] or 0,
        task["durationMs"] or 0,
    ),
    reverse=True,
)


report = {
    "applicationId": application_id,
    "tag": tag,
    "jobs": [
        {
            "jobId": job["jobId"],
            "name": job.get("name"),
            "status": job.get("status"),
            "stageIds": job.get("stageIds", []),
            "numTasks": job.get("numTasks"),
            "numCompletedTasks": job.get("numCompletedTasks"),
            "numFailedTasks": job.get("numFailedTasks"),
            "numKilledTasks": job.get("numKilledTasks"),
            "submissionTime": job.get("submissionTime"),
            "completionTime": job.get("completionTime"),
            "jobTags": job.get("jobTags", []),
        }
        for job in jobs
    ],
    "sqlExecutions": [
        {
            "id": sql.get("id"),
            "status": sql.get("status"),
            "description": sql.get("description"),
            "durationMs": sql.get("duration"),
            "runningJobIds": sql.get("runningJobIds", []),
            "successJobIds": sql.get("successJobIds", []),
            "failedJobIds": sql.get("failedJobIds", []),
            "planDescription": sql.get("planDescription"),
            "nodes": sql.get("nodes", []),
        }
        for sql in sql_executions
    ],
    "stages": stage_reports,
    "executors": executor_reports,
    "taskHotspots": task_reports[:25],
    "metricNotes": {
        "cpuEfficiency": "executor CPU milliseconds / executor run milliseconds",
        "gcFraction": "JVM GC milliseconds / executor run milliseconds",
        "schedulerDelayFraction": "scheduler delay / total task duration",
        "shuffleFetchWaitFraction": "shuffle fetch wait / executor run time",
        "spillRatio": "memory plus disk spill bytes / input plus shuffle-read bytes",
        "shuffleAmplification": "shuffle read plus write bytes / source input bytes",
        "processedMiBPerCpuSecond": "input plus shuffle-read MiB / executor CPU second",
        "taskSkew": "compare p95 or max with the median task duration and bytes",
        "workShare": "executor task runtime / all tagged task runtime",
        "approximateParallelEfficiency": (
            "task executor runtime / wall time / cores on involved executors; "
            "shared-server contention can lower this value"
        ),
        "applicationLifetimePeakMemoryMetrics": (
            "shared-application lifetime peak; do not attribute it solely to this tag"
        ),
    },
}

print(json.dumps(report, indent=2))
