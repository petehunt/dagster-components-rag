from dataclasses import dataclass
from typing import Optional
from dagster import AssetExecutionContext, Definitions, asset
from dagster_components import (
    Component,
    ComponentLoadContext,
    DefaultComponentScaffolder,
    ResolvableSchema,
)

from ragster.lib.daglake import BaseDaglakeResource


class CollectionIngest(ResolvableSchema):
    dep: str
    to_collection_name: str
    id_column: str
    document_column: str
    from_url: Optional[str] = None


class ChromaDBSchema(ResolvableSchema):
    path: str
    ingests: list[CollectionIngest]


@dataclass
class ChromaDB(Component):
    """COMPONENT SUMMARY HERE.

    COMPONENT DESCRIPTION HERE.
    """

    path: str
    ingests: list[CollectionIngest]

    @classmethod
    def get_schema(cls):
        return ChromaDBSchema

    @classmethod
    def get_scaffolder(cls) -> DefaultComponentScaffolder:
        return DefaultComponentScaffolder()

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        import chromadb
        import duckdb

        def build_asset_for_ingest(ingest: CollectionIngest):
            @asset(
                name=f"chromadb_ingest_{ingest.to_collection_name}",
                required_resource_keys={"daglake"},
                deps=[ingest.dep],
            )
            def ingest_asset(context: AssetExecutionContext):
                daglake: BaseDaglakeResource = context.resources.daglake

                db = duckdb.connect(":memory:")
                for init_sql in daglake.get_duckdb_init_sql():
                    db.execute(init_sql)

                from_url = ingest.from_url
                if from_url is None:
                    from_url = f"daglake://dep/{ingest.dep}"

                db.execute(
                    daglake.get_duckdb_import_sql(
                        ingest.to_collection_name,
                        daglake.resolve_daglake_url(context, from_url),
                    )
                )

                client = chromadb.PersistentClient(path=self.path)
                collection = client.get_or_create_collection(
                    name=ingest.to_collection_name
                )

                rows = db.sql(
                    f"select {ingest.id_column} as id, {ingest.document_column} as document from {ingest.to_collection_name}"
                )

                while chunk := rows.fetchmany(128):
                    collection.upsert(
                        ids=[str(row[0]) for row in chunk],
                        documents=[str(row[1]) for row in chunk],
                    )

            return ingest_asset

        assets = [build_asset_for_ingest(ingest) for ingest in self.ingests]

        return Definitions(assets=assets)
