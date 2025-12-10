from __future__ import annotations

import click
import json
import textwrap
from flask.cli import with_appcontext
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_db import db
from rich.console import Console
from rich.table import Table
from typing import Any
from invenio_jobs.models import Job, Run


@click.group(name="jobs")
def jobs_cli() -> None:
    """invenio jobs commands."""


@jobs_cli.command("list")
@with_appcontext
def list_jobs() -> None:
    """List invenio jobs."""

    jobs = db.session.query(Job).all()

    console = Console()
    table = Table(
        title="Invenio Jobs", show_header=True, header_style="bold magenta"
    )
    table.add_column("ID", style="cyan")
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


@jobs_cli.command("create")
@click.option("--title", required=True, help="Job title")
@click.option("--task", required=True, help="Task name")
@with_appcontext
def create_job(title: str, task: str) -> None:
    """Create invenio job."""
    job_options = [
        ProcessRORAffiliationsJob,
        ProcessRORFundersJob,
        ImportAwardsOpenAIREJob,
        UpdateAwardsCordisJob,
        ImportORCIDJob
    ]
    try:
        job_data = {
            "title": title,
            "active": True,
            "task": task,
            # "task": "Load ROR Affiliations",
            "default_queue": "celery"
        }

        job = Job(
            title=title,
            active=active,
            description=description,
            task=task,
            default_queue=default_queue
        )
        db.session.add(job)
        db.session.commit()

        console = Console()
        console.print(
            f"[green]✓[/green] Job '{title}' created successfully with ID: {id}"
        )
    except Exception as e:
        db.session.rollback()
        click.echo(f"Error creating job: {e}", err=True)
        raise


@jobs_cli.command("delete")
@click.option("--title", required=True, help="Job title")
@with_appcontext
def delete_job(title: str) -> None:
    """Delete invenio job."""
    if not yes:
        click.confirm(
            f"Are you sure you want to delete job '{title}'?",
            abort=True,
        )
    try:
        job = Job.query.get(title=title)
        if job:
            db.session.delete(job)
            db.session.commit()
    except Exception as e:
        db.session.rollback()
        click.echo(f"Error deleting job: {e}", err=True)
        raise


@jobs_cli.command("runs")
@click.option("--title", required=True, help="Job title")
@with_appcontext
def list_job_runs() -> None:
    """List runs of an invenio job."""

    try:
        job = Job.query.get(title=title)
        runs = Run.query.filter_by(job_id=job.id).all()

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


@jobs_cli.command("log")
@click.option("--title", required=True, help="Job title")
@click.option("--run-id", help="Run ID")
@with_appcontext
def print_run_log(title: str, run_id: str) -> None:
    """Print log of an invenio job run."""
    try:
        if run_id:
            run = Run.query.get(run_id)
        else:
            job = Job.query.get(title=title)
            run = job.last_run
        print(run.log)
    except Exception as e:
        db.session.rollback()
        click.echo(f"Error getting job run: {e}", err=True)
        raise
