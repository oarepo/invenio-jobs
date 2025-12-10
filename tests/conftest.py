# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# Invenio-Jobs is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Pytest configuration.

See https://pytest-invenio.readthedocs.io/ for documentation on which test
fixtures are available.
"""

from types import SimpleNamespace

import pytest
from flask_principal import AnonymousIdentity
from invenio_access.models import ActionRoles
from invenio_access.permissions import any_user as any_user_need
from invenio_accounts.models import Role
from invenio_administration.permissions import administration_access_action
from invenio_app.factory import create_api
from invenio_records_permissions.generators import AnyUser, SystemProcess
from invenio_records_permissions.policies import BasePermissionPolicy

from invenio_jobs.proxies import current_jobs_service


@pytest.fixture(scope="module")
def extra_entry_points():
    """Extra entry points to load the mock_module features."""
    # __import__("ipdb").set_trace()
    return {
        "invenio_jobs.jobs": [
            "mock_module = mock_module.jobs:MockJob",
        ],
        "invenio_celery.tasks": [
            "mock_module = mock_module.tasks",
        ],
    }


@pytest.fixture(scope="module")
def app_config(app_config):
    """Application config override."""

    class MockPermissionPolicy(BasePermissionPolicy):
        can_search = [AnyUser(), SystemProcess()]
        can_create = [AnyUser()]
        can_read = [AnyUser()]
        can_update = [AnyUser()]
        can_delete = [AnyUser()]
        can_stop = [AnyUser()]

    app_config["REST_CSRF_ENABLED"] = False

    app_config["JOBS_TASKS_PERMISSION_POLICY"] = MockPermissionPolicy
    app_config["JOBS_PERMISSION_POLICY"] = MockPermissionPolicy
    app_config["JOBS_RUNS_PERMISSION_POLICY"] = MockPermissionPolicy
    app_config["APP_LOGS_PERMISSION_POLICY"] = MockPermissionPolicy
    app_config["THEME_FRONTPAGE"] = False
    app_config["CELERY_TASK_ALWAYS_EAGER"] = True
    app_config["CELERY_TASK_EAGER_PROPAGATES"] = True
    return app_config


@pytest.fixture(scope="module")
def create_app(instance_path, entry_points):
    """Application factory fixture."""
    return create_api


#
# Users and identities
#
@pytest.fixture()
def anon_identity(user):
    """Anonymous user."""
    identity = AnonymousIdentity()
    identity.id = user.id  # Mock it with the user id
    identity.provides.add(any_user_need)
    return identity


@pytest.fixture()
def user(UserFixture, app, db):
    """User meant to test permissions."""
    u = UserFixture(
        email="user@inveniosoftware.org",
        username="user",
        password="user",
        user_profile={
            "full_name": "User",
            "affiliations": "CERN",
        },
        active=True,
        confirmed=True,
    )
    u.create(app, db)
    return u


@pytest.fixture()
def jobs(db, anon_identity):
    """Job fixtures."""
    common_data = {
        "task": "update_expired_embargos",
        "default_queue": "low",
        "default_args": {
            "arg1": "value1",
            "arg2": "value2",
            "kwarg1": "value3",
        },
    }
    interval_job = current_jobs_service.create(
        anon_identity,
        {
            "title": "Test interval job",
            "schedule": {
                "type": "interval",
                "hours": 4,
            },
            **common_data,
        },
    )
    crontab_job = current_jobs_service.create(
        anon_identity,
        {
            "title": "Test crontab job",
            "schedule": {
                "type": "crontab",
                "minute": "0",
                "hour": "0",
            },
            **common_data,
        },
    )
    simple_job = current_jobs_service.create(
        anon_identity,
        {
            "title": "Test unscheduled job",
            **common_data,
        },
    )
    return SimpleNamespace(
        interval=interval_job,
        crontab=crontab_job,
        simple=simple_job,
    )
