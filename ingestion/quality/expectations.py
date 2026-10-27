from __future__ import annotations

from pathlib import Path
from typing import Any

import great_expectations as gx
import pandas as pd
from great_expectations import expectations as gxe


class BaseQA:
    """Run Great Expectations suites against in-memory pandas DataFrames."""

    def __init__(
        self,
        df: pd.DataFrame,
        expected_columns: set[str],
        data_docs_dir: str | Path | None = None,
    ):
        self.df = df
        self.expected_columns = expected_columns
        self.data_docs_dir = Path(data_docs_dir) if data_docs_dir else None

    def column_expectations(self) -> list[Any]:
        # Always validate the contract itself. Only add null checks for columns
        # that exist, so a missing-column failure is clear instead of cascading
        # into unrelated GX exceptions for the same absent column.
        expectations: list[Any] = [
            gxe.ExpectTableColumnsToMatchSet(
                column_set=sorted(self.expected_columns),
                exact_match=True,
            )
        ]
        for column in sorted(self.expected_columns.intersection(self.df.columns)):
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
        # An ephemeral context keeps this lightweight batch validation isolated:
        # it uses GX suites and validation results without creating local stores.
        context = gx.get_context(mode="ephemeral")
        datasource = context.data_sources.add_pandas(name="validation_datasource")
        asset = datasource.add_dataframe_asset(name="validation_dataframe")
        batch_definition = asset.add_batch_definition_whole_dataframe(
            name="whole_dataframe"
        )

        suite = gx.ExpectationSuite(name=suite_name)
        for expectation in expectations:
            suite.add_expectation(expectation)
        context.suites.add(suite)

        validation_definition = gx.ValidationDefinition(
            data=batch_definition,
            suite=suite,
            name=f"{suite_name}_validation",
        )
        context.validation_definitions.add(validation_definition)
        validation_result = validation_definition.run(
            batch_parameters={"dataframe": dataframe}
        )

        if self.data_docs_dir is not None:
            # GX renders a browsable report for this suite and its validation
            # result; use a separate folder so each suite/stage stays available.
            site_path = (self.data_docs_dir / suite_name).resolve()
            context.add_data_docs_site(
                site_name="validation_docs",
                site_config={
                    "class_name": "SiteBuilder",
                    "show_how_to_buttons": True,
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": str(site_path),
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder"
                    },
                },
            )
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

    def run_all_checks(self) -> list[dict[str, Any]]:
        return self.validate(
            suite_name="base_data_quality",
            dataframe=self.df,
            expectations=self.column_expectations(),
        )
