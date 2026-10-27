from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import great_expectations as gx
import pandas as pd
from great_expectations import expectations as gxe


_DOCS_CONTEXTS: dict[Path, tuple[Any, Any]] = {}


class BaseQA:
    """Run Great Expectations suites against in-memory pandas DataFrames."""

    def __init__(
        self,
        df: pd.DataFrame,
        expected_columns: Iterable[str],
        data_docs_dir: str | Path | None = None,
    ):
        self.df = df
        # Preserve the declared schema order for GX's validation report TOC.
        # Sets remain supported for callers that do not provide an ordered schema.
        self.expected_columns = (
            tuple(sorted(expected_columns))
            if isinstance(expected_columns, (set, frozenset))
            else tuple(expected_columns)
        )
        self.data_docs_dir = Path(data_docs_dir) if data_docs_dir else None

    def column_expectations(self) -> list[Any]:
        # Always validate the contract itself. Only add null checks for columns
        # that exist, so a missing-column failure is clear instead of cascading
        # into unrelated GX exceptions for the same absent column.
        expectations: list[Any] = [
            gxe.ExpectTableColumnsToMatchOrderedList(
                column_list=list(self.expected_columns),
            )
        ]
        for column in self.expected_columns:
            if column not in self.df.columns:
                continue
            expectations.append(
                gxe.ExpectColumnValuesToNotBeNull(column=column)
            )
        return expectations

    def validate(
        self,
        *,
        suite_name: str,
        dataframe: pd.DataFrame,
        expectations: list[Any],
    ) -> list[dict[str, Any]]:
        docs_root = self._docs_root() if self.data_docs_dir is not None else None
        if docs_root is None:
            context, batch_definition = self._new_validation_context()
        else:
            context, batch_definition = self._docs_context(docs_root)

        # A suite is a reusable GX object, but each data quality run is a
        # separate validation. Unique suite names let Data Docs show each
        # dataset/stage result together in one native GX site.
        report_parts = (
            self.data_docs_dir.resolve().relative_to(docs_root).parts
            if docs_root is not None and self.data_docs_dir is not None
            else ()
        )
        report_label = "_".join(report_parts).replace("-", "_")
        suite_id = f"{suite_name}_{report_label}_{uuid4().hex[:8]}"
        suite = gx.ExpectationSuite(name=suite_id)
        for expectation in expectations:
            suite.add_expectation(expectation)
        context.suites.add(suite)

        validation_definition = gx.ValidationDefinition(
            data=batch_definition,
            suite=suite,
            name=f"{suite_id}_validation",
        )
        context.validation_definitions.add(validation_definition)
        validation_result = validation_definition.run(
            batch_parameters={"dataframe": dataframe}
        )

        if docs_root is not None:
            # The same context, result stores, and filesystem site are shared by
            # all suites/stages in this process, so GX builds one complete index.
            docs_url = context.build_data_docs(
                site_names=["validation_docs"]
            )["validation_docs"]
            print(f"Great Expectations Data Docs: {docs_url}")

        # Preserve GX's full expectation configuration and result payload in the
        # existing audit-log format, while keeping a readable expectation label.
        results = []
        for item in validation_result.to_json_dict()["results"]:
            config = item["expectation_config"]
            kwargs = config.get("kwargs", {})
            expectation_name = config["type"]
            if "column" in kwargs:
                expectation_name += f":{kwargs['column']}"

            results.append(
                {
                    "expectation": expectation_name,
                    "success": item["success"],
                    "severity": config.get("severity", "critical"),
                    "result": item.get("result", {}),
                    "expectation_config": config,
                    "exception_info": item.get("exception_info", {}),
                    "checked_at": pd.Timestamp.now(tz="UTC"),
                }
            )
        return results

    def _new_validation_context(self) -> tuple[Any, Any]:
        """Create an isolated context when Data Docs output is disabled."""
        context = gx.get_context(mode="ephemeral")
        datasource = context.data_sources.add_pandas(name="validation_datasource")
        asset = datasource.add_dataframe_asset(name="validation_dataframe")
        batch_definition = asset.add_batch_definition_whole_dataframe(
            name="whole_dataframe"
        )
        return context, batch_definition

    def _docs_root(self) -> Path:
        """Resolve the one Data Docs root shared by every pipeline validation."""
        if self.data_docs_dir is None:
            raise ValueError("Data Docs output is not configured.")
        return next(
            (
                parent.resolve()
                for parent in (
                    self.data_docs_dir,
                    *self.data_docs_dir.parents,
                )
                if parent.name == "data_docs"
            ),
            self.data_docs_dir.resolve(),
        )

    @staticmethod
    def _docs_context(docs_root: Path) -> tuple[Any, Any]:
        """Reuse one GX context and native Data Docs site per output root."""
        cached = _DOCS_CONTEXTS.get(docs_root)
        if cached is not None:
            return cached

        context = gx.get_context(mode="ephemeral")
        datasource = context.data_sources.add_pandas(name="validation_datasource")
        asset = datasource.add_dataframe_asset(name="validation_dataframe")
        batch_definition = asset.add_batch_definition_whole_dataframe(
            name="whole_dataframe"
        )
        context.add_data_docs_site(
            site_name="validation_docs",
            site_config={
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": str(docs_root),
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder"
                },
            },
        )
        cached = (context, batch_definition)
        _DOCS_CONTEXTS[docs_root] = cached
        return cached

    def run_all_checks(self) -> list[dict[str, Any]]:
        return self.validate(
            suite_name="base_data_quality",
            dataframe=self.df,
            expectations=self.column_expectations(),
        )
