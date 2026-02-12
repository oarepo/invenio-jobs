# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
# Copyright (C) 2024 Graz University of Technology.
# Copyright (C) 2025 KTH Royal Institute of Technology.
# Copyright (c) 2025 CESNET z.s.p.o.
#
# Invenio-Jobs is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Service results."""

import json
from collections.abc import Iterable, Sized

from invenio_i18n import gettext as _
from invenio_records_resources.services.records.results import (
    RecordItem,
    RecordList,
)

from invenio_jobs.models import Job
from invenio_jobs.utils import job_arg_json_dumper

from ..api import AttrDict

try:
    # flask_sqlalchemy<3.0.0
    from flask_sqlalchemy import Pagination
except ImportError:
    # flask_sqlalchemy>=3.0.0
    from flask_sqlalchemy.pagination import Pagination


class Item(RecordItem):
    """Single item result."""

    @property
    def id(self):
        """Get the result id."""
        return str(self._record.id)


class JobItem(Item):
    """Single Job result."""

    @property
    def data(self):
        """Data representation of job result item."""
        if self._data:
            return self._data

        job_dict = self._obj.dump()
        if self._obj.last_run:
            job_dict["last_run"] = self._obj.last_run.dump()
            job_dict["last_runs"] = self._obj.last_runs
        job_dict["default_args"] = json.dumps(
            self._obj.default_args, default=job_arg_json_dumper
        )
        job_record = AttrDict(job_dict)

        self._data = self._schema.dump(
            job_record,
            context={
                "identity": self._identity,
                "record": self._record,
            },
        )

        if self._links_tpl:
            self._data["links"] = self.links
        return self._data


class List(RecordList):
    """List result."""

    @property
    def items(self):
        """Iterator over the items."""
        if isinstance(self._results, Pagination):
            return self._results.items
        elif isinstance(self._results, Iterable):
            return self._results
        return self._results

    @property
    def total(self):
        """Get total number of hits."""
        if hasattr(self._results, "hits"):
            return self._results.hits.total["value"]
        if isinstance(self._results, Pagination):
            return self._results.total
        elif isinstance(self._results, Sized):
            return len(self._results)
        else:
            return None

    # TODO: See if we need to override this
    @property
    def aggregations(self):
        """Get the search result aggregations."""
        try:
            return self._results.labelled_facets.to_dict()
        except AttributeError:
            return None

    @property
    def hits(self):
        """Iterator over the hits."""
        for hit in self.items:
            # Project the hit
            hit_dict = hit.dump()
            hit_record = AttrDict(hit_dict)
            projection = self._schema.dump(
                hit_record,
                context=dict(identity=self._identity, record=hit),
            )
            if self._links_item_tpl:
                projection["links"] = self._links_item_tpl.expand(self._identity, hit)
            if self._nested_links_item:
                for link in self._nested_links_item:
                    link.expand(self._identity, hit, projection)

            yield projection


class JobList(List):
    """List result."""

    @property
    def hits(self):
        """Iterator over the hits."""
        for hit in self.items:
            # Project the hit
            job_dict = hit.dump()
            job_dict["last_run"] = hit.last_run
            job_dict["last_runs"] = hit.last_runs
            job_dict["default_args"] = json.dumps(
                hit.default_args, default=job_arg_json_dumper
            )
            job_record = AttrDict(job_dict)
            projection = self._schema.dump(
                job_record,
                context=dict(identity=self._identity, record=hit),
            )
            if self._links_item_tpl:
                projection["links"] = self._links_item_tpl.expand(self._identity, hit)
            if self._nested_links_item:
                for link in self._nested_links_item:
                    link.expand(self._identity, hit, projection)

            yield projection


class AppLogsList(List):
    """List result."""

    @property
    def hits(self):
        """Iterator over the hits."""
        for hit in self.items:
            # Project the hit
            projection = self._schema.dump(
                hit,
                context=dict(identity=self._identity),
            )
            if self._links_item_tpl:
                projection["links"] = self._links_item_tpl.expand(self._identity, hit)
            if self._nested_links_item:
                for link in self._nested_links_item:
                    link.expand(self._identity, hit, projection)

            yield projection

    def to_dict(self):
        """Return result as a dictionary."""
        res = super().to_dict()
        if self._results:
            sort = self._results.hits.hits[
                -1
            ].sort  # We want to keep the sort field of the last item to resume search from here with search_after
            if sort:
                res["hits"]["sort"] = list(sort)
            if hasattr(self._results, "_truncated") and self._results._truncated:
                res["warnings"] = [
                    {
                        "message": _(
                            "Too many log results returned (%(total)s). "
                            "Only the most recent %(max)s results are shown.",
                            total=self._results._total_available,
                            max=self._results._max_docs,
                        ),
                        "type": "truncated_results",
                        "total_available": self._results._total_available,
                        "max_results": self._results._max_docs,
                    }
                ]
        return res
