from pathlib import Path

from dagster_components import build_component_defs

from ragster.lib.daglake import LocalDaglakeResource

blobstore_path = Path(__file__).parent / "data"
blobstore_path.mkdir(parents=True, exist_ok=True)

defs = build_component_defs(
    components_root=Path(__file__).parent / "defs",
    resources={"daglake": LocalDaglakeResource(path=str(blobstore_path))},
)