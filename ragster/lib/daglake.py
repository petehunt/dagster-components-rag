from typing import Optional
import dagster as dg
from abc import ABC, abstractmethod
from urllib.parse import urlparse
from pathlib import Path


class BaseDaglakeResource(ABC):
    @abstractmethod
    def _get_base_url(self) -> str: ...

    def _get_asset_url(self, asset_key: str, partition_key: Optional[str]) -> str:
        url = f"{self._get_base_url()}/{asset_key}"
        if partition_key is not None:
            url = f"{url}/{partition_key}"
        return url + ".parquet"

    def resolve_daglake_url(self, context: dg.AssetExecutionContext, uri: str) -> str:
        parsed = urlparse(uri)
        if parsed.scheme == "daglake":
            partition_key = None
            if context.has_partition_key:
                partition_key = context.partition_key

            if parsed.netloc == "asset":
                return self._get_asset_url(
                    context.asset_key.to_user_string(), partition_key
                )

            if parsed.netloc != "dep":
                raise ValueError(f"Invalid daglake uri: {uri}")

            input_names = [input_def.name for input_def in context.op_def.input_defs]
            dep_asset_key_to_partition_keys = {}
            for input_name in input_names:
                asset_key = context.asset_key_for_input(input_name)
                partition_keys = []
                if context.has_partition_key:
                    partition_keys = context.asset_partition_keys_for_input(input_name)

                dep_asset_key_to_partition_keys[asset_key.to_user_string()] = (
                    partition_keys
                )

            target_dep_key = parsed.path[1:]
            if target_dep_key not in dep_asset_key_to_partition_keys:
                raise ValueError(f"Invalid daglake uri: {uri}")

            target_partition_keys = dep_asset_key_to_partition_keys[target_dep_key]
            if len(target_partition_keys) == 0:
                target_partition_key = None
            elif len(target_partition_keys) == 1:
                target_partition_key = target_partition_keys[0]
            else:
                raise ValueError(f"Invalid daglake uri: {uri}")

            return self._get_asset_url(target_dep_key, target_partition_key)
        return uri

    @abstractmethod
    def get_duckdb_init_sql(self) -> list[str]: ...

    def get_duckdb_export_sql(self, table_name: str, url: str) -> str:
        return f"COPY {table_name} TO '{url}' (FORMAT PARQUET)"

    def get_duckdb_import_sql(self, table_name: str, url: str) -> str:
        return f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{url}')"


class S3DaglakeResource(BaseDaglakeResource, dg.ConfigurableResource):
    key_id: str
    secret: str
    region: str
    bucket: str
    prefix: Optional[str] = None

    def _get_base_url(self) -> str:
        base_url = f"s3://{self.bucket}/"
        if self.prefix is not None:
            base_url = f"{base_url}{self.prefix}/"
        return base_url

    def get_duckdb_init_sql(self) -> list[str]:
        return [
            "INSTALL httpfs",
            "LOAD httpfs",
            f"""
CREATE SECRET (
    TYPE s3,
    KEY_ID '{self.key_id}',
    SECRET '{self.secret}',
    REGION '{self.region}'
)
""",
        ]


class LocalDaglakeResource(BaseDaglakeResource, dg.ConfigurableResource):
    path: str

    def _get_base_url(self) -> str:
        path = Path(self.path)
        return path.absolute().as_uri()

    def _get_asset_url(self, asset_key: str, partition_key: Optional[str]) -> str:
        url = super()._get_asset_url(asset_key, partition_key)
        parsed = urlparse(url)
        if parsed.scheme == "file":
            path = Path(parsed.path)
            path.parent.mkdir(parents=True, exist_ok=True)
        return url

    def get_duckdb_init_sql(self) -> list[str]:
        return []
