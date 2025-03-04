from dataclasses import dataclass
from typing import Optional
from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_components import (
    Component,
    ComponentLoadContext,
    DefaultComponentScaffolder,
    ResolvableSchema,
)

from ragster.lib.daglake import BaseDaglakeResource


class PyairbyteStream(ResolvableSchema):
    stream: str
    to_url: Optional[str] = None


class PyAirbyteSchema(ResolvableSchema):
    asset_key: Optional[str] = None
    source: str
    config: dict
    streams: list[PyairbyteStream]


@dataclass
class PyAirbyte(Component):
    """COMPONENT SUMMARY HERE.

    COMPONENT DESCRIPTION HERE.
    """

    source: str
    config: dict
    streams: list[PyairbyteStream]

    @classmethod
    def get_schema(cls):
        return PyAirbyteSchema

    @classmethod
    def get_scaffolder(cls) -> DefaultComponentScaffolder:
        return DefaultComponentScaffolder()

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        print("got config", self.config)
        def build_asset_for_stream(stream_def: PyairbyteStream):
            stream = stream_def.stream

            @asset(
                name=f"airbyte_{self.source}_{stream}",
                required_resource_keys={"daglake"},
            )
            def airbyte_run(context: AssetExecutionContext):
                daglake: BaseDaglakeResource = context.resources.daglake

                import airbyte as ab

                source = ab.get_source(
                    f"source-{self.source}",
                    install_if_missing=True,
                    config=self.config,
                )
                source.check()
                source.select_streams([stream])
                cache = ab.get_default_cache()
                source.read(cache=cache)
                cache.execute_sql(daglake.get_duckdb_init_sql())

                to_url = stream_def.to_url
                if to_url is None:
                    to_url = "daglake://asset"
                asset_uri = daglake.resolve_daglake_url(context, to_url)

                export_sql = daglake.get_duckdb_export_sql(
                    stream,
                    asset_uri,
                )
                cache.execute_sql(export_sql)
                return MaterializeResult(
                    metadata={
                        "daglake.asset_uri": MetadataValue.url(asset_uri),
                    }
                )

            return airbyte_run

        return Definitions(
            assets=[build_asset_for_stream(stream) for stream in self.streams]
        )
