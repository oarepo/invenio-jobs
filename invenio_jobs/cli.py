# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# Invenio-Jobs is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Invenio Jobs CLI"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import click
from celery.schedules import crontab
from flask.cli import with_appcontext
from invenio_access.permissions import system_identity
from invenio_db import db
from rich.console import Console
from rich.table import Table

from invenio_jobs.models import Job, Run, RunStatusEnum
from invenio_jobs.proxies import current_jobs, current_jobs_logs_service

LOG_LEVEL_STYLE = {
    "ERROR": "red",
    "WARNING": "yellow",
    "INFO": "green", }

def validate_celery_crontab(ctx, param, value: str):
    """Validates crontab format."""
    parts = value.split()
    if len(parts) != 5:
        raise click.BadParameter(
            "Expected 5 fields: minute hour day_of_month month day_of_week "
            "(e.g. '*/5 * * * *')"
        )

    minute, hour, dom, month, dow = parts

    try:
        # This will raise if any field is invalid / not supported by Celery crontab
        schedule = crontab(
            minute=minute,
            hour=hour,
            day_of_month=dom,
            month_of_year=month,
            day_of_week=dow,
        )
    except Exception as e:
        raise click.BadParameter(f"Invalid Celery crontab: {e}")

    return schedule  # returning the schedule object can be handy


def next_n_datetimes(schedule, start: datetime, n: int):
    """Gets next n datetimes from schedule."""
    t = start
    out = []
    for _ in range(n):
        delta = schedule.remaining_estimate(t)  # timedelta until next run after t :contentReference[oaicite:1]{index=1}
        # If we're exactly on a boundary, remaining_estimate can be 0; nudge forward.
        if delta.total_seconds() <= 0:
            t = t + timedelta(seconds=1)
            delta = schedule.remaining_estimate(t)
        t = t + delta
        out.append(t)
    return out


def _check_task_name(task_name: str):
    """Checks if task name exists in job registry."""
    try:
        if current_jobs.registry.get(task_name):
            return True
    except:
        return False


def _get_job(job_id):
    """Tries to get Job first by Job.id, then by Job.title."""
    try:
        return db.session.query(Job).filter_by(id=job_id).first()
    except Exception as e:
        return db.session.query(Job).filter_by(title=job_id).first()


def _get_run(instance_id):
    """Tries to get Run by Run.id, then tries to get Job by Job.id and returns Job.last_run."""
    try:
        return db.session.query(Run).filter_by(id=instance_id).first()
    except Exception as e:
        job = db.session.query(Job).filter_by(id=instance_id).first()
        return job.last_run


def _get_logs(pid_value):
    """Try to get logs by Run.id (taken from RunsDetailsView._get_logs)"""
    """Retrieve and format logs."""
    params = dict(q=f'"{pid_value}"')
    logs_result = current_jobs_logs_service.search(system_identity, params)
    result_dict = logs_result.to_dict()
    logs = result_dict["hits"]["hits"]
    sort = result_dict["hits"].get("sort")
    warnings = result_dict.get("warnings", [])

    return logs, sort, warnings


@click.group(name="jobs")
def jobs() -> None:
    """invenio jobs commands."""


@jobs.command("types")
@with_appcontext
def list_job_types() -> None:
    """List job task types in job registry. When creating a job, task name must exist in job registry."""
    job_types = current_jobs.registry.get_all()

    console = Console()
    table = Table(
        title="Invenio Jobs", show_header=True, header_style="bold magenta"
    )
    table.add_column("Title", style="green")
    table.add_column("Task name", style="cyan")
    for job_type in job_types.values():
        table.add_row(
            str(job_type.title),
            str(job_type.id),
        )

    console.print(table)


@jobs.command("list")
@with_appcontext
def list_jobs() -> None:
    """List Jobs."""

    jobs = db.session.query(Job).all()

    console = Console()
    table = Table(
        title="Invenio Jobs", show_header=True, header_style="bold magenta"
    )
    table.add_column("ID", style="cyan", no_wrap=True)
    table.add_column("Title", style="green")
    table.add_column("Queue", style="blue")
    table.add_column("Task")
    table.add_column("Active")
    table.add_column("Description", max_width=50)
    for job in jobs:
        table.add_row(
            str(job.id),
            str(job.title),
            str(job.default_queue),
            str(job.task),
            str(job.active),
            str(job.description or ""),
        )

    console.print(table)


@jobs.command("create")
@click.option("--title", required=True, help="Job title")
@click.option("--task", required=True, help="Task name")
@with_appcontext
def create_job(title: str, task: str) -> None:
    """Create Job.
    Task name must match against job type in registry."""
    try:
        if _check_task_name(task) is False:
            click.echo(f"Task name not in jobs registry: {task}", err=True)
            return
        job_data = {
            "title": title,
            "active": True,
            "task": task,
            # "task": "Load ROR Affiliations",
            "default_queue": "celery"
        }

        # match task in current_jobs.registry.get(task):

        job = Job(
            **job_data
        )
        db.session.add(job)
        db.session.commit()

        console = Console()
        console.print(
            f"[green]✓[/green] Job '{title}' created successfully with ID: {job.id}"
        )
    except Exception as e:
        db.session.rollback()
        click.echo(f"Error creating job: {e}", err=True)
        raise


@jobs.command("delete")
@click.argument("job_id")
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
@with_appcontext
def delete_job(job_id: str, yes: bool) -> None:
    """Delete Job by id."""
    if not yes:
        click.confirm(
            f"Are you sure you want to delete job '{job_id}'?",
            abort=True,
        )
    try:
        job = _get_job(job_id)
        if job:
            db.session.delete(job)
            db.session.commit()
            console = Console()
            console.print(
                f"[green]✓[/green] Job '{job_id}' deleted successfully."
            )
    except Exception as e:
        db.session.rollback()
        click.echo(f"Error deleting job: {e}", err=True)
        raise


@jobs.command("runs")
@click.argument("job_id")
# @click.option("--title", required=True, help="Job title")
@with_appcontext
def list_job_runs(job_id: str) -> None:
    """List Job Runs."""

    try:
        runs = db.session.query(Run).filter_by(job_id=job_id).all()

        console = Console()
        table = Table(
            title="Invenio Job Runs", show_header=True, header_style="bold magenta"
        )
        table.add_column("ID", min_width=40, style="cyan")
        table.add_column("Started At", style="green")
        table.add_column("Finished At", style="blue")
        table.add_column("Status")
        table.add_column("Message", max_width=50)
        for run in runs:
            table.add_row(
                str(run.id),
                str(run.started_at or "Run hasn't started yet"),
                str(run.finished_at or "Run hasn't finished yet"),
                str(run.status.name.lower()),
                str(run.message or ""),
            )

        console.print(table)
    except Exception as e:
        db.session.rollback()
        click.echo(f"Error listing job runs: {e}", err=True)
        raise


def get_run_summary(run: Run) -> dict:
    """Collect Run task and subtasks summary."""
    run_summary = {"total_subtasks": run.total_subtasks,
                      "completed_subtasks": run.completed_subtasks,
                      "failed_subtasks": run.failed_subtasks,
                      "errored_entries": run.errored_entries,
                      "inserted_entries": run.inserted_entries,
                      "updated_entries": run.updated_entries,
                      "total_entries": run.total_entries
                      }
    for subtask in run.substasks:
        if subtask:
            summary = get_run_summary(subtask)
            for key, value in summary.items():
                run_summary[key] += value
    return run_summary


def print_run_log_table(instance_id: str):
    run = _get_run(instance_id)
    run_summary = {"total_subtasks": run.total_subtasks,
                   "completed_subtasks": run.completed_subtasks,
                   "failed_subtasks": run.failed_subtasks,
                   "errored_entries": run.errored_entries,
                   "inserted_entries": run.inserted_entries,
                   "updated_entries": run.updated_entries,
                   "total_entries": run.total_entries
                   }

    console = Console()
    table = Table(
        title="Invenio Run Log", show_header=True, header_style="bold magenta"
    )
    table.add_column("Log Messages", style="green")

    logs, sort, warnings = _get_logs(instance_id)
    table.add_row(f"[bold cyan]Run Summary:")
    for k, v in run_summary.items():
        table.add_row(f"{k}: {v}", style="cyan")
    table.add_section()
    for row in logs:
        table.add_row(f"[{row['timestamp']}] {row['level']}: {row['message']}", style=LOG_LEVEL_STYLE.get(row["level"], "green"))
    console.print(table)

@jobs.command("log")
@click.argument("instance_id")
@click.option("-f", "--follow", is_flag=True, help="Follow run log until finished")
@with_appcontext
def print_run_log(instance_id: str, follow: bool = False, interval: int=1) -> None:
    """Print log of a job run."""
    try:
        while True:
            os.system("clear")
            run = _get_run(instance_id)
            if run is None:
                click.echo(f"Run not found for ID: {instance_id}", err=True)
                break
            print_run_log_table(instance_id)
            # if run.status not in (RunStatusEnum.QUEUED, RunStatusEnum.RUNNING, RunStatusEnum.CANCELLING):
            if run.status in (RunStatusEnum.SUCCESS, RunStatusEnum.PARTIAL_SUCCESS, RunStatusEnum.FAILED, RunStatusEnum.CANCELLED, RunStatusEnum.WARNING):
                break
            if not follow:
                break
            time.sleep(interval)
    except Exception as e:
        click.echo(f"Error getting job run: {e}", err=True)
        raise


@jobs.command("run")
@click.argument("job_id")
@with_appcontext
def create_run_for_job(job_id):
    """Create a run for a job."""
    try:
        job = _get_job(job_id)
        run = Run.create(job, queue="celery")
        db.session.add(run)
        db.session.commit()
        console = Console()
        console.print(
            f"[green]✓[/green] Run for'{job.title}' created successfully with ID: {run.id}"
        )
    except Exception as e:
        db.session.rollback()
        click.echo(f"Error creating job run: {e}", err=True)


@jobs.command("update")
@click.argument("job_id")
@with_appcontext
def update_job(job_id):
    """set job parameters
    Does nothing right now.
    dependent on PR: https://github.com/inveniosoftware/invenio-jobs/pull/110
    """
    pass


@jobs.command("schedule")
@click.argument("job_id")
@click.option("--schedule", required=True, callback=validate_celery_crontab, help="Job schedule (crontab format)")
@click.option("--tz", default="Europe/Prague", show_default=True)
@click.option("--yes", is_flag=True, help="Skip confirmation prompt")
def schedule_job(job_id, schedule, tz, yes):
    """Schedule a job using crontab format."""
    now = datetime.now(ZoneInfo(tz))
    upcoming = next_n_datetimes(schedule, now, 3)

    click.echo(f"Now: {now.isoformat()}")

    console = Console()
    table = Table(
        title="Job Schedule", show_header=True, header_style="bold magenta"
    )
    table.add_column("Run Nr", style="green")
    table.add_column("Scheduled Time", style="cyan")
    for i, dt in enumerate(upcoming, 1):
        table.add_row(
            str(i),
            str(dt),
        )

    console.print(table)

    if not yes:
        click.confirm(
            f"Do you want use this schedule?",
            abort=True,
        )
        crontab_schedule = crontab(schedule)
        job = _get_job(job_id)
        job.schedule(crontab_schedule)
