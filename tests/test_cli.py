import pytest

from invenio_jobs.cli import list_jobs, create_job, delete_job, list_job_types, list_job_runs, print_run_log, \
    schedule_job, update_job, create_run_for_job
from invenio_jobs.proxies import current_jobs_logs_service
from datetime import datetime, timezone
from invenio_jobs.api import AttrDict

def _make_hit(idx):
    """Create a fake search hit."""
    timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() + idx
    sort_value = [timestamp, f"id-{idx}"]
    hit = AttrDict(
        {
            "@timestamp": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(),
            "level": "ERROR",
            "message": f"log-{idx}",
            "module": "tests",
            "function": "fn",
            "line": idx,
            "context": {
                "job_id": "job-123",
                "run_id": "run-456",
                "identity_id": "user-789",
            },
            "sort": sort_value,
        }
    )
    hit.meta = AttrDict({"sort": sort_value})
    return hit


class FakeHits(list):
    """List-like container mimicking an OpenSearch hits collection."""

    def __init__(self, hits, total):
        super().__init__(hits)
        self.hits = self  # mimic .hits attribute used by the service
        self.total = {"value": total}


class FakeResponse:
    """Response object mimicking OpenSearch DSL responses."""

    def __init__(self, hits, total):
        self.hits = FakeHits(hits, total)

    def __iter__(self):
        """Iterate over hits like elasticsearch-dsl responses."""
        return iter(self.hits)


class FakeSearch:
    """Very small fake Search implementation for unit testing."""

    def __init__(self, hits):
        self._hits = list(hits)
        self._cursor = 0
        self._params = {}
        self.execute_calls = 0

    def _clone(self):
        clone = FakeSearch(self._hits)
        clone._cursor = self._cursor
        clone._params = dict(self._params)
        return clone

    def count(self):
        return len(self._hits)

    def sort(self, *args, **kwargs):
        return self

    def extra(self, **kwargs):
        self._params.update(kwargs)
        return self

    def execute(self):
        self.execute_calls += 1
        size = self._params.get("size", max(len(self._hits) - self._cursor, 0))
        start = self._cursor
        end = min(start + size, len(self._hits))
        if start >= len(self._hits):
            page_hits = []
        else:
            page_hits = self._hits[start:end]
            self._cursor = end
        return FakeResponse(list(page_hits), len(self._hits))



@pytest.mark.usefixtures("app")
def test_jobs_cli(monkeypatch, app, client):
    service = current_jobs_logs_service
    created_searches = []

    hits = [_make_hit(idx) for idx in range(8, 0, -1)]

    def fake_search(self, *args, **kwargs):
        search = FakeSearch(hits)
        created_searches.append(search)
        return search

    monkeypatch.setattr(service.__class__, "_search", fake_search)

    runner = app.test_cli_runner()

    # list job types
    result = runner.invoke(list_job_types)
    assert result.exit_code == 0
    assert (result.output ==
            ('                    Invenio Jobs                     \n'
             '┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┓\n'
             '┃ Title                   ┃ Task name               ┃\n'
             '┡━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━┩\n'
             '│ Update expired embargos │ update_expired_embargos │\n'
             '└─────────────────────────┴─────────────────────────┘\n'))

    # create job
    create_job_result = runner.invoke(create_job, args=["--title", "test", "--task", "update_expired_embargos"])
    assert create_job_result.exit_code == 0
    assert create_job_result.output.startswith("✓ Job 'test' created successfully with ID")
    job_id = create_job_result.output.replace("\n", "").split("ID: ")[-1]

    # list jobs
    list_jobs_result = runner.invoke(list_jobs)
    assert list_jobs_result.exit_code == 0
    assert (list_jobs_result.output ==
            ('                                  Invenio '
             'Jobs                                  \n'
             '┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┓\n'
             '┃ ID                                   ┃ Title ┃ Queue ┃ Task  ┃ Acti… ┃ '
             'Desc… ┃\n'
             '┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━┩\n'
             f'│ {job_id} │ test  │ cele… │ upda… │ True  '
             '│       │\n'
             '└──────────────────────────────────────┴───────┴───────┴───────┴───────┴───────┘\n'))

    # list empty job runs
    list_empty_job_runs_result = runner.invoke(list_job_runs, args=[job_id])
    assert list_empty_job_runs_result.exit_code == 0
    assert (list_empty_job_runs_result.output ==
            ('                                     Invenio Job Runs                           \n'
             '┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┳\n'
             '┃ ID                                       ┃ Started At ┃ Finished At ┃ Status ┃\n'
             '┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━╇\n'
             '└──────────────────────────────────────────┴────────────┴─────────────┴────────┴\n'))

    # create job run
    create_run_for_job_result = runner.invoke(create_run_for_job, args=[job_id])
    run_id = create_run_for_job_result.output.replace("\n", "").split("ID: ")[-1]
    assert create_run_for_job_result.exit_code == 0
    assert create_run_for_job_result.output.endswith(f"created successfully with ID: \n{run_id}\n")

    # list non-empty job runs
    list_job_runs_result = runner.invoke(list_job_runs, args=[job_id])
    assert list_job_runs_result.exit_code == 0
    assert (list_job_runs_result.output ==
            ('                                           Invenio Job '
             'Runs                     \n'
             '┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━\n'
             '┃ ID                                       ┃ Started At        ┃ Finished '
             'At    \n'
             '┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━\n'
             f"│ {run_id}     │ Run hasn't        │ Run "
             "hasn't     \n"
             '│                                          │ started yet       │ finished '
             'yet   \n'
             '└──────────────────────────────────────────┴───────────────────┴────────────────\n'))

    # schedule job
    schedule_result = runner.invoke(schedule_job, args=[job_id, "--schedule", "1 1 1 * *", "--yes"])
    assert schedule_result.exit_code == 0
    assert "Job Schedule" in schedule_result.output
    assert "Scheduled Time" in schedule_result.output

    print_run_log_result = runner.invoke(print_run_log, args=[run_id])
    assert print_run_log_result.exit_code == 0
    assert (print_run_log_result.output ==
('              Invenio Run Log               \n'
 '┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓\n'
 '┃ Log Messages                             ┃\n'
 '┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩\n'
 '│ Run Summary:                             │\n'
 '│ total_subtasks: 0                        │\n'
 '│ completed_subtasks: 0                    │\n'
 '│ failed_subtasks: 0                       │\n'
 '│ errored_entries: 0                       │\n'
 '│ inserted_entries: 0                      │\n'
 '│ updated_entries: 0                       │\n'
 '│ total_entries: 0                         │\n'
 '├──────────────────────────────────────────┤\n'
 '│ [2025-01-01T00:00:08+00:00] ERROR: log-8 │\n'
 '│ [2025-01-01T00:00:07+00:00] ERROR: log-7 │\n'
 '│ [2025-01-01T00:00:06+00:00] ERROR: log-6 │\n'
 '│ [2025-01-01T00:00:05+00:00] ERROR: log-5 │\n'
 '│ [2025-01-01T00:00:04+00:00] ERROR: log-4 │\n'
 '│ [2025-01-01T00:00:03+00:00] ERROR: log-3 │\n'
 '│ [2025-01-01T00:00:02+00:00] ERROR: log-2 │\n'
 '│ [2025-01-01T00:00:01+00:00] ERROR: log-1 │\n'
 '└──────────────────────────────────────────┘\n'))

    # delete job
    delete_job_result = runner.invoke(delete_job, args=[job_id, "--yes"])
    assert delete_job_result.exit_code == 0
    assert delete_job_result.output.startswith(f"✓ Job '{job_id}' deleted successfully")


def test_update_job(app):
    """set job parameters
    Does nothing right now.
    dependent on PR: https://github.com/inveniosoftware/invenio-jobs/pull/110
    """
    runner = app.test_cli_runner()
    result = runner.invoke(update_job, args="jobid")
    assert result.exit_code == 0
