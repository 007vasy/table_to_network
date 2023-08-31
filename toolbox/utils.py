import os
import logging
from pathlib import Path
from dataclasses import dataclass
import json
import polars as pl

from typing import Dict

LOGLEVEL_KEY = 'LOGLEVEL'
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    level=os.environ.get(LOGLEVEL_KEY, 'INFO').upper(),
    datefmt='%Y-%m-%d %H:%M:%S',
)

ROOT_DIR = Path(__file__).parent.parent

ATTRIBUTES = Dict[str, str]


@dataclass
class Abstract2ColMap:
    attributes: ATTRIBUTES

    def _validate_attributes(self):
        for k, v in self.attributes.items():
            if not isinstance(k, str):
                raise ValueError(f'attribute {k} should be a string')

            if not isinstance(v, str):
                raise ValueError(f'attribute {v} should be a string')

    def __post_init__(self):
        self._validate_attributes()


@dataclass
class Node2ColMap(Abstract2ColMap):
    id: str
    attributes: ATTRIBUTES


@dataclass
class Edge2ColMap(Abstract2ColMap):
    source: str
    target: str
    attributes: ATTRIBUTES


NODES_MAP = Dict[str, Node2ColMap]
EDGES_MAP = Dict[str, Edge2ColMap]


@dataclass
class File2NetworkMap:
    nodes: NODES_MAP
    edges: EDGES_MAP


FILES2NETWORKMAP = Dict[str, File2NetworkMap]
FOLDER2NETWORKMAP = Dict[str, FILES2NETWORKMAP]


@dataclass
class Config:
    folder2networkmap: FOLDER2NETWORKMAP


def load_json(path: Path) -> Dict:
    with open(path) as f:
        return json.load(f)


def parse_config(raw_config: Dict) -> Config:
    folder2networkmap: Dict = {}
    for folder, folder_networkmap in raw_config.items():
        if folder not in folder2networkmap:
            folder2networkmap[folder] = {}
        for file, file_networkmap in folder_networkmap.items():
            if file not in folder2networkmap[folder]:
                folder2networkmap[folder][file] = {}
            raw_nodes = file_networkmap.get('nodes', {})
            raw_edges = file_networkmap.get('edges', {})

            nodes: NODES_MAP = {}
            edges: EDGES_MAP = {}

            for node_type, node_map in raw_nodes.items():
                nodes[node_type] = Node2ColMap(
                    id=node_map['id'],
                    attributes=node_map['attributes']
                )

            for edge_type, edge_map in raw_edges.items():
                edges[edge_type] = Edge2ColMap(
                    source=edge_map['source'],
                    target=edge_map['target'],
                    attributes=edge_map['attributes']
                )

            folder2networkmap[folder][file] = File2NetworkMap(
                nodes=nodes,
                edges=edges
            )
    return Config(
        folder2networkmap=folder2networkmap
    )


def get_config(config_path: Path) -> Config:
    raw_config = load_json(config_path)
    return parse_config(raw_config)


def dummy(a):
    return a + 1


def extract_node_type_from_table(table: pl.DataFrame, node2colmap: Node2ColMap) -> pl.DataFrame:
    node_id = node2colmap.id
    node_attributes = node2colmap.attributes

    id_df = table.select(node_id).rename({node_id: 'id'})

    rename_attributes = {v: k for k, v in node_attributes.items()}

    attributes_df = table.select(
        node_attributes.values()).rename(rename_attributes)

    node_table = pl.concat([id_df, attributes_df], how='horizontal')

    return node_table.unique()
