import os
import logging
from pathlib import Path
from dataclasses import dataclass
import json

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
class Node2ColMap:
    id: str
    attributes: ATTRIBUTES


@dataclass
class Edge2ColMap:
    source: str
    target: str
    attributes: ATTRIBUTES


@dataclass
class File2NetworkMap:
    nodes: Node2ColMap
    edges: Edge2ColMap


FOLDER2NETWORKMAP = Dict[str, File2NetworkMap]


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
            nodes = file_networkmap['nodes']
            edges = file_networkmap['edges']
            folder2networkmap[folder][file] = File2NetworkMap(
                nodes=Node2ColMap(
                    id=nodes['id'],
                    attributes=nodes['attributes']
                ),
                edges=Edge2ColMap(
                    source=edges['source'],
                    target=edges['target'],
                    attributes=edges['attributes']
                )
            )
    return Config(
        folder2networkmap=folder2networkmap
    )


def get_config(config_path: Path) -> Config:
    raw_config = load_json(config_path)
    return parse_config(raw_config)


def dummy(a):
    return a + 1
