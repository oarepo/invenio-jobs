# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# Invenio-Jobs is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Models."""
import contextlib
import enum
import json
import uuid
from copy import deepcopy
from datetime import timedelta

import sqlalchemy as sa
from celery.schedules import crontab
from invenio_accounts.models import User
from invenio_db import db
from invenio_users_resources.records import UserAggregate
from sqlalchemy import event, func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import aliased
from sqlalchemy_utils import Timestamp
from sqlalchemy_utils.types import ChoiceType, JSONType, UUIDType
from werkzeug.utils import cached_property

from invenio_jobs.proxies import current_jobs
from invenio_jobs.utils import job_arg_json_dumper

JSON = (
    db.JSON()
    .with_variant(postgresql.JSONB(none_as_null=True), "postgresql")
    .with_variant(JSONType(), "sqlite")
    .with_variant(JSONType(), "mysql")
)


def _dump_dict(model):
    """Dump a model to a dictionary."""
    return {c.key: getattr(model, c.key) for c in sa.inspect(model).mapper.column_attrs}


class Job(db.Model, Timestamp):
    """Job model."""

    __tablename__ = "jobs_job"

    id = db.Column(UUIDType, primary_key=True, default=uuid.uuid4)
    active = db.Column(db.Boolean, default=True, nullable=False)
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    task = db.Column(db.String(255))
    default_queue = db.Column(db.String(64))
    schedule = db.Column(JSON, nullable=True)

    @cached_property
    def last_run(self):
        """Last run of the job."""
        _run = self.runs.order_by(Run.created.desc()).first()
        return _run if _run else {}

    @cached_property
    def last_runs(self):
        """Last run of the job."""
        _runs = {}
        last_runs_queries_list = []
        for status in RunStatusEnum:
            _runs[status.name.lower()] = {}
            last_runs_queries_list.append(
                db.select(Run)
                .where(Run.job_id == self.id)
                .where(Run.status == status)
                .order_by(Run.created.desc())
                .limit(1)
            )

        query = db.union(*last_runs_queries_list)
        orm_stmt = select(Run).from_statement(query)
        for run in db.session.execute(orm_stmt).scalars():
            _runs[run.status.name.lower()] = run
        return _runs

    @classmethod
    def bulk_load_last_runs(cls, jobs) -> None:
        """Bulk load Job last_runs and save it on the jobs.

        Loading last runs in bulk avoids N+1 queries when accessing
        the last_runs property on multiple Job instances. This method
        populates the cached_property `last_runs` on each Job instance
        in the provided list.

        :param jobs: List of Job instances to load last runs for.
        """
        job_ids = [job.id for job in jobs]

        ranked = (
            select(
                Run,
                func.row_number()
                .over(
                    partition_by=(Run.job_id, Run.status), order_by=Run.created.desc()
                )
                .label("rn"),
            )
            .where(Run.job_id.in_(job_ids))
            .subquery()
        )

        latest_run = aliased(Run, ranked)

        query = select(latest_run).where(ranked.c.rn == 1)

        status_dict = {status.name.lower(): {} for status in RunStatusEnum}
        job_by_id = {job.id: job for job in jobs}
        jobs_last_runs = {job_id: status_dict.copy() for job_id in job_ids}

        for run in db.session.execute(query).scalars():
            jobs_last_runs[run.job_id][run.status.name.lower()] = run

        for job_id, runs in jobs_last_runs.items():
            job = job_by_id[job_id]
            # fill in the cached_property of last_runs so that it doesn't
            # trigger a new query when accessed
            job.last_runs = runs
            job.last_run = max(runs, key=lambda r: r.created, default={})

    @property
    def default_args(self):
        """Compute default job arguments."""
        return Task.get(self.task)._build_task_arguments(job_obj=self)

    @property
    def parsed_schedule(self):
        """Return schedule parsed as crontab or timedelta."""
        if not self.schedule:
            return None

        schedule = deepcopy(self.schedule)
        stype = schedule.pop("type")
        if stype == "crontab":
            return crontab(**schedule)
        elif stype == "interval":
            return timedelta(**schedule)

    def dump(self):
        """Dump the job as a dictionary."""
        return _dump_dict(self)


@event.listens_for(Job, "load")
@event.listens_for(Job, "refresh")
@event.listens_for(Job, "expire")
def clear_cache(target, *args):
    """Clear Job.last_runs cached_property.

    As the last_runs is a cached_property, we need to clear it
    when the Job instance is loaded/refreshed/expired to avoid
    stale data.
    """
    with contextlib.suppress(AttributeError, KeyError):
        del target.last_runs
    with contextlib.suppress(AttributeError, KeyError):
        del target.last_run


class RunStatusEnum(enum.Enum):
    """Enumeration of a run's possible states."""

    QUEUED = "Q"
    RUNNING = "R"
    SUCCESS = "S"
    FAILED = "F"
    WARNING = "W"
    CANCELLING = "C"
    CANCELLED = "X"
    PARTIAL_SUCCESS = "P"


class Run(db.Model, Timestamp):
    """Run model."""

    __tablename__ = "jobs_run"

    id = db.Column(UUIDType, primary_key=True, default=uuid.uuid4)

    job_id = db.Column(UUIDType, db.ForeignKey(Job.id))
    job = db.relationship(Job, backref=db.backref("runs", lazy="dynamic"))

    started_by_id = db.Column(db.Integer, db.ForeignKey(User.id), nullable=True)
    _started_by = db.relationship(User)

    @property
    def started_by(self):
        """Return UserAggregate of the user that started the run."""
        if self._started_by:
            return UserAggregate.from_model(self._started_by)

    started_at = db.Column(db.DateTime, nullable=True)
    finished_at = db.Column(db.DateTime, nullable=True)

    task_id = db.Column(UUIDType, nullable=True)
    status = db.Column(
        ChoiceType(RunStatusEnum, impl=db.String(1)),
        nullable=False,
        default=RunStatusEnum.QUEUED.value,
    )

    message = db.Column(db.Text, nullable=True)

    title = db.Column(db.Text, nullable=True)
    args = db.Column(JSON, default=lambda: dict(), nullable=True)
    queue = db.Column(db.String(64), nullable=False)

    parent_run_id = db.Column(
        UUIDType, db.ForeignKey("jobs_run.id"), index=True, nullable=True
    )
    subtasks = db.relationship(
        "Run",
        backref=db.backref("parent_run", remote_side=[id]),
        cascade="all, delete-orphan",
        lazy="dynamic",
    )
    # Meant to mark if the sibtasks of this run have been all spawned.
    subtasks_closed = db.Column(db.Boolean, default=False, nullable=False)

    total_subtasks = db.Column(db.Integer, default=0, nullable=False)
    completed_subtasks = db.Column(db.Integer, default=0, nullable=False)
    failed_subtasks = db.Column(db.Integer, default=0, nullable=False)
    errored_entries = db.Column(db.Integer, default=0, nullable=False)
    inserted_entries = db.Column(db.Integer, default=0, nullable=False)
    updated_entries = db.Column(db.Integer, default=0, nullable=False)
    total_entries = db.Column(db.Integer, default=0, nullable=False)

    @classmethod
    def create(cls, job, **kwargs):
        """Create a new run."""
        if "args" not in kwargs:
            kwargs["args"] = cls.generate_args(job)
        else:
            task_arguments = deepcopy(kwargs["args"])
            kwargs["args"] = cls.generate_args(job, task_arguments=task_arguments)
        if "queue" not in kwargs:
            kwargs["queue"] = job.default_queue
        return cls(job=job, **kwargs)

    @classmethod
    def generate_args(cls, job, task_arguments=None):
        """Generate new run args."""
        args = Task.get(job.task)._build_task_arguments(
            job_obj=job, **task_arguments or {}
        )

        args = json.dumps(args, default=job_arg_json_dumper)
        args = json.loads(args)

        return args

    def dump(self):
        """Dump the run as a dictionary."""
        from invenio_jobs.services.schema import JobArgumentsSchema

        dict_run = _dump_dict(self)
        serialized_args = JobArgumentsSchema().load({"args": dict_run["args"]})

        dict_run["args"] = serialized_args
        return dict_run


class Task:
    """Celery Task model."""

    _all_tasks = None

    def __init__(self, obj):
        """Initialize model."""
        self._obj = obj

    def __getattr__(self, name):
        """Proxy attribute access to the task object."""
        # TODO: See if we want to limit what attributes are exposed
        return getattr(self._obj, name)

    @cached_property
    def description(self):
        """Return description."""
        if not self._obj.__doc__:
            return ""
        return self._obj.__doc__.split("\n")[0]

    @classmethod
    def all(cls):
        """Return all tasks."""
        return current_jobs.jobs

    @classmethod
    def get(cls, id_):
        """Get registered task by id."""
        return cls(current_jobs.registry.get(id_))
