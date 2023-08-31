import os
import logging
from pathlib import Path
from dataclasses import dataclass
import json
import polars as pl
import glob
from tqdm import tqdm

from typing import Dict, List

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


NODECOLMAPS = List[Node2ColMap]
EDGECOLMAPS = List[Edge2ColMap]

NODES_MAP = Dict[str, NODECOLMAPS]
EDGES_MAP = Dict[str, EDGECOLMAPS]


@dataclass
class File2NetworkMap:
    nodes: NODES_MAP
    edges: EDGES_MAP


FILES2NETWORKMAP = Dict[str, File2NetworkMap]
FOLDER2NETWORKMAP = Dict[str, FILES2NETWORKMAP]


@dataclass
class Config:
    folder2networkmap: FOLDER2NETWORKMAP


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

            for node_type, node_maps in raw_nodes.items():
                nodes[node_type] = []
                for node_map in node_maps:
                    nodes[node_type].append(Node2ColMap(
                        id=node_map['id'],
                        attributes=node_map['attributes']
                    )
                    )

            for edge_type, edge_maps in raw_edges.items():
                edges[edge_type] = []
                for edge_map in edge_maps:
                    edges[edge_type].append(Edge2ColMap(
                        source=edge_map['source'],
                        target=edge_map['target'],
                        attributes=edge_map['attributes']
                    )
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


def load_json(path: Path) -> Dict:
    with open(path) as f:
        return json.load(f)


def create_label_file_name(_label: str, extension: str = 'parquet') -> str:
    return f'{_label}.{extension}'


def extract_node_type_from_table(table: pl.DataFrame, node2colmap: Node2ColMap) -> pl.DataFrame:
    node_id = node2colmap.id
    node_attributes = node2colmap.attributes

    id_df = table.select(node_id).rename({node_id: 'id'})

    rename_attributes = {v: k for k, v in node_attributes.items()}

    attributes_df = table.select(
        node_attributes.values()).rename(rename_attributes)

    node_table = pl.concat([id_df, attributes_df], how='horizontal')

    return node_table.unique()


def extract_and_merge_x_type(table: pl.DataFrame, output_dir: Path, _label: str, _colmap: Abstract2ColMap) -> None:

    file_path = output_dir / create_label_file_name(_label)

    if isinstance(_colmap, Node2ColMap):
        extracted_table = extract_node_type_from_table(table, _colmap)
    elif isinstance(_colmap, Edge2ColMap):
        extracted_table = extract_edge_type_from_table(table, _colmap)
    else:
        raise ValueError(f'unknown type {_colmap}')

    if file_path.exists():
        # if the file exist read it in and merge it with the new data
        existing_table = pl.read_parquet(file_path)
        merged_table = pl.concat(
            [existing_table, extracted_table], how='vertical')
        merged_table = merged_table.unique()
    else:
        merged_table = extracted_table

    # save the file
    merged_table.write_parquet(file_path)

    logging.debug(f'file saved to {file_path}')


def extract_edge_type_from_table(table: pl.DataFrame, edge2colmap: Edge2ColMap) -> pl.DataFrame:
    source = edge2colmap.source
    target = edge2colmap.target
    edge_attributes = edge2colmap.attributes

    source_df = table.select(source).rename({source: 'source'})
    target_df = table.select(target).rename({target: 'target'})

    rename_attributes = {v: k for k, v in edge_attributes.items()}

    attributes_df = table.select(
        edge_attributes.values()).rename(rename_attributes)

    edge_table = pl.concat(
        [source_df, target_df, attributes_df], how='horizontal')

    return edge_table.unique()


def extract_from_file(source_file_path: Path, output_dir: Path, file2networkmap: File2NetworkMap) -> None:
    table = pl.read_parquet(source_file_path)

    for node_type, node_colmaps in file2networkmap.nodes.items():
        for node_colmap in node_colmaps:
            extract_and_merge_x_type(table, output_dir, node_type, node_colmap)

    for edge_type, edge_colmaps in file2networkmap.edges.items():
        for edge_colmap in edge_colmaps:
            extract_and_merge_x_type(table, output_dir, edge_type, edge_colmap)

# for key, value in tqdm(d.items(), desc='Processing keys'):
#     tqdm.write(f'Current key: {key}')


def extract_from_folder(source_folder_path: Path, output_dir: Path, folder2networkmap: FOLDER2NETWORKMAP) -> None:
    for folder, files2networkmap in tqdm(folder2networkmap.items(), desc='Processing folders'):
        source_folder = source_folder_path / folder
        for file, file2networkmap in tqdm(files2networkmap.items(), desc=f'Files from folder {folder}', leave=False):
            tqdm.write(f'File(s): {file}')
            # allow for wildcards
            for filepath in glob.glob(str(source_folder / file)):
                source_file_path = Path(filepath)
                extract_from_file(source_file_path,
                                  output_dir, file2networkmap)


def show_folder_files(folder: Path) -> None:
    files = [f for f in folder.iterdir() if f.is_file()]

    # create a bullet list
    files_str = '\n'.join([f'- {f}' for f in files])
    logging.info(f'Files in folder {folder}:\n{files_str}')
