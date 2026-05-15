# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
# Copyright (C) 2025-2026 Graz University of Technology.
#
# Invenio-Jobs is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Models."""

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
from sqlalchemy.dialects import postgresql
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


def _job_has_custom_arguments_set(job):
    return job.run_args and job.run_args.get("custom_args")


class Job(db.Model, db.Timestamp):
    """Job model."""

    __tablename__ = "jobs_job"

    id = db.Column(UUIDType, primary_key=True, default=uuid.uuid4)
    active = db.Column(db.Boolean, default=True, nullable=False)
    title = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    task = db.Column(db.String(255))
    default_queue = db.Column(db.String(64))
    schedule = db.Column(JSON, nullable=True)
    run_args = db.Column(JSON, nullable=True)
    notifications = db.Column(JSON, nullable=True, default=None)

    @property
    def last_run(self):
        """Last run of the job."""
        _run = self.runs.order_by(Run.created.desc()).first()
        return _run if _run else {}

    @property
    def last_runs(self):
        """Last run of the job."""
        _runs = {}
        for status in RunStatusEnum:
            run = (
                self.runs.filter_by(status=status).order_by(Run.created.desc()).first()
            )
            _runs[status.name.lower()] = run if run else {}
        return _runs

    @property
    def default_args(self):
        """Compute default job arguments."""
        custom_args = None
        if self.run_args:
            custom_args = self.run_args.get("custom_args")
        return Task.get(self.task)._build_task_arguments(
            custom_args=custom_args, job_obj=self
        )

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

    def set_run_args(self, value):
        """Custom setter for run_args.

        If Job has already custom_args set and the new value is empty,
        keep the args in DB.
        """
        if self.run_args:
            current_custom_args = self.run_args.get("custom_args")
            if not value.get("custom_args") and current_custom_args:
                value["custom_args"] = current_custom_args
        self.run_args = value

    def dump(self):
        """Dump the job as a dictionary."""
        return _dump_dict(self)


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


class Run(db.Model, db.Timestamp):
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

    started_at = db.Column(db.UTCDateTime, nullable=True)
    finished_at = db.Column(db.UTCDateTime, nullable=True)

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

    total_subtasks = db.Column(
        db.Integer, default=0, server_default="0", nullable=False
    )
    completed_subtasks = db.Column(
        db.Integer, default=0, server_default="0", nullable=False
    )
    failed_subtasks = db.Column(
        db.Integer, default=0, server_default="0", nullable=False
    )
    errored_entries = db.Column(
        db.Integer, default=0, server_default="0", nullable=False
    )
    inserted_entries = db.Column(
        db.Integer, default=0, server_default="0", nullable=False
    )
    updated_entries = db.Column(
        db.Integer, default=0, server_default="0", nullable=False
    )
    total_entries = db.Column(db.Integer, default=0, server_default="0", nullable=False)

    @classmethod
    def create(cls, job, **kwargs):
        """Create a new run."""
        if "args" not in kwargs and not _job_has_custom_arguments_set(job):
            kwargs["args"] = cls.generate_args(job)
        elif (
            "args" not in kwargs or not kwargs.get("args").get("custom_args")
        ) and _job_has_custom_arguments_set(job):
            task_arguments = job.run_args
            kwargs["args"] = cls.generate_args(job, task_arguments=task_arguments)
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
