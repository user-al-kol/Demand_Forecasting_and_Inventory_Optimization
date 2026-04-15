import os
from dataclasses import dataclass
from typing import Callable, List


LOG_DIR= os.environ.get("LOG_DIR")
SOURCE_DIR = os.environ.get("SOURCE_DIR")
LOGICAL_DATE = os.environ.get("LOGICAL_DATE")
IM_DESTINATION_DIR = os.environ.get("IM_DESTINATION_DIR")
S_DESTINATION_DIR = os.environ.get("S_DESTINATION_DIR")
INVENTORY_MOVEMENT_SOURCE = os.environ.get("IM_SOURCE_DIR")
IM_SOURCE_DIR = os.environ.get("IM_SOURCE_DIR")
S_SOURCE_DIR = os.environ.get("S_SOURCE_DIR")
DELTA_PATH = os.environ.get("DELTA_PATH")


@dataclass
class DatasetConfig:
    file: str
    destination_dir: str
    source_partitioned: str
    table: str
    schema_fn: Callable
    schema_null: Callable
    keys: List[str]
    entity: str