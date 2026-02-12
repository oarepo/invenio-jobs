# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
# Copyright (c) 2025 CESNET z.s.p.o.
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

    # last_run_id for each status in RunStatusEnum
    last_run_id = db.Column(UUIDType, db.ForeignKey("jobs_run.id"))
    last_queued_run_id = db.Column(UUIDType, db.ForeignKey("jobs_run.id"))
    last_running_run_id = db.Column(UUIDType, db.ForeignKey("jobs_run.id"))
    last_success_run_id = db.Column(UUIDType, db.ForeignKey("jobs_run.id"))
    last_failed_run_id = db.Column(UUIDType, db.ForeignKey("jobs_run.id"))
    last_warning_run_id = db.Column(UUIDType, db.ForeignKey("jobs_run.id"))
    last_cancelling_run_id = db.Column(UUIDType, db.ForeignKey("jobs_run.id"))
    last_cancelled_run_id = db.Column(UUIDType, db.ForeignKey("jobs_run.id"))
    last_partial_success_run_id = db.Column(UUIDType, db.ForeignKey("jobs_run.id"))

    # # canonical ownership rel
    # runs = db.relationship(
    #     "Run",
    #     back_populates="job",
    #     foreign_keys="Run.job_id",
    #     cascade="all, delete-orphan",
    # )
    runs = db.relationship(
        "Run",
        back_populates="job",
        foreign_keys="Run.job_id",
        lazy="dynamic",
    )

    # pointer rels (explicit FK each time)
    last_run_ref = db.relationship("Run", foreign_keys=[last_run_id], post_update=True)
    last_queued_run_ref = db.relationship("Run", foreign_keys=[last_queued_run_id], post_update=True)
    last_running_run_ref = db.relationship("Run", foreign_keys=[last_running_run_id], post_update=True)
    last_success_run_ref = db.relationship("Run", foreign_keys=[last_success_run_id], post_update=True)
    last_failed_run_ref = db.relationship("Run", foreign_keys=[last_failed_run_id], post_update=True)
    last_warning_run_ref = db.relationship("Run", foreign_keys=[last_warning_run_id], post_update=True)
    last_cancelling_run_ref = db.relationship("Run", foreign_keys=[last_cancelling_run_id], post_update=True)
    last_cancelled_run_ref = db.relationship("Run", foreign_keys=[last_cancelled_run_id], post_update=True)
    last_partial_success_run_ref = db.relationship("Run", foreign_keys=[last_partial_success_run_id], post_update=True)

    @property
    def last_run(self):
        """Last run of the job."""
        if self.last_run_id is None:
            return {}
        return self.runs.get(self.last_run_id)

    @property
    def last_runs(self):
        """Last runs of the job."""
        last_runs_dict = {
            "cancelled": {},
            "cancelling": {},
            "failed": {},
            "partial_success": {},
            "queued": {},
            "running": {},
            "success": {},
            "warning": {},
        }
        if self.last_run_id is None:
            return last_runs_dict
        last_runs = self.runs.filter(
            [
                self.last_queued_run_id,
                self.last_running_run_id,
                self.last_success_run_id,
                self.last_failed_run_id,
                self.last_warning_run_id,
                self.last_cancelling_run_id,
                self.last_cancelled_run_id,
                self.last_partial_success_run_id,
            ]
        )
        for run in last_runs:
            last_runs_dict[run.status.name.lower()] = run
        return last_runs_dict

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

    # job_id = db.Column(UUIDType, db.ForeignKey(Job.id))
    # job = db.relationship(Job, backref=db.backref("runs", lazy="dynamic"))
    job_id = db.Column(UUIDType, db.ForeignKey("jobs_job.id"), nullable=False)
    job = db.relationship("Job", back_populates="runs", foreign_keys=[job_id])

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
