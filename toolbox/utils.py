import os
import logging
from pathlib import Path
from dataclasses import dataclass
import json
import polars as pl
import glob
from tqdm import tqdm

from typing import Dict, List, Any

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

    def get_needed_cols(self) -> List[str]:
        raise NotImplementedError()


@dataclass
class Node2ColMap(Abstract2ColMap):
    id: str
    attributes: ATTRIBUTES

    def get_needed_cols(self) -> List[str]:
        return [self.id] + list(self.attributes.values())


@dataclass
class Edge2ColMap(Abstract2ColMap):
    source: str
    target: str
    attributes: ATTRIBUTES

    def get_needed_cols(self) -> List[str]:
        return [self.source, self.target] + list(self.attributes.values())


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


def make_dir(path: Path) -> None:
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)


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

    return node_table.unique(subset=['id'], keep='last')


def convert_dfs_to_schema(dfs: List[pl.DataFrame], schema: Dict[str, Any]) -> List[pl.DataFrame]:
    converted_dfs = []

    for df in dfs:
        logging.debug('before convert schema', df.schema)
        for field, field_data_type in schema.items():
            column_name = field
            target_type = field_data_type
            if df.schema[field] != target_type:
                df = df.select([pl.col(column_name).cast(
                    target_type) if column == column_name else column for column in df.columns])
        converted_dfs.append(df)

    for df in converted_dfs:
        logging.debug('after convert schema', df.schema)

    return converted_dfs


class MergeDataFrameError(Exception):
    pass


class SchemaMismatchError(MergeDataFrameError):
    pass


def merge_dfs(dfs: List[pl.DataFrame]) -> pl.DataFrame:

    # Get schemas
    schemas = [df.schema for df in dfs]

    # Pick schema with less utf8 columns types
    utf8_count = [len([1 for field_data_type in schema.values()
                      if field_data_type == pl.Utf8]) for schema in schemas]

    if utf8_count[0] < utf8_count[1]:
        target_schema, backup_schema = schemas
    else:
        target_schema, backup_schema = schemas[::-1]

    # Convert columns in all dataframes to match target_schema
    try:
        converted_dfs = convert_dfs_to_schema(dfs, target_schema)
    except Exception as e:
        logging.warning(
            f'Could not convert columns to match target schema. Using backup schema instead. Error: {e}')
        converted_dfs = convert_dfs_to_schema(dfs, backup_schema)

    # Merge dataframes
    try:
        merged_df = pl.concat(converted_dfs, how='vertical').unique()
    except Exception as e:
        logging.error(f'Could not merge dataframes. Error: {e}')

        for df in converted_dfs:
            logging.error(f'df schema: {df.schema}')
            logging.error(f'df head: {df.head()}')

        raise MergeDataFrameError(str(e)) from e

    return merged_df


def is_needed_cols_in_table(table_cols: List[str], cols: List[str]) -> bool:
    for col in cols:
        if col not in table_cols:
            return False

    return True


def extract_and_merge_x_type(table: pl.DataFrame, output_dir: Path, _label: str, _colmap: Abstract2ColMap) -> None:

    file_path = output_dir / create_label_file_name(_label)

    if not is_needed_cols_in_table(table.columns, _colmap.get_needed_cols()):
        raise SchemaMismatchError(
            f'Table schema: {table.schema}, needed cols: {_colmap.get_needed_cols()}')

    if isinstance(_colmap, Node2ColMap):
        extracted_table = extract_node_type_from_table(table, _colmap)
    elif isinstance(_colmap, Edge2ColMap):
        extracted_table = extract_edge_type_from_table(table, _colmap)
    else:
        raise ValueError(f'unknown type {_colmap}')

    if file_path.exists():
        # if the file exist read it in and merge it with the new data
        existing_table = pl.read_parquet(file_path)
        logging.debug('\n existing table >>\n')
        logging.debug(existing_table.head())
        logging.debug('\n extracted table >>\n')
        logging.debug(extracted_table.head())
        logging.debug('\n end >>\n')
        merged_table = merge_dfs([existing_table, extracted_table])

    else:
        merged_table = extracted_table

    # save the file
    merged_table.write_parquet(file_path)

    logging.debug(f'file saved or updated to > {file_path}')


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

    return edge_table.unique(subset=['source', 'target'], keep='last')


class ExtractError(Exception):
    pass


class NodeExtractError(ExtractError):
    pass


class EdgeExtractError(ExtractError):
    pass


def extract_from_file(source_file_path: Path, output_dir: Path, file2networkmap: File2NetworkMap) -> None:
    table = pl.read_parquet(source_file_path)

    for node_type, node_colmaps in file2networkmap.nodes.items():
        for node_colmap in node_colmaps:
            try:
                extract_and_merge_x_type(
                    table, output_dir, node_type, node_colmap)
            except MergeDataFrameError as e:
                raise NodeExtractError(
                    f'Failed to process > {source_file_path}. {node_type} {node_colmap} {str(e)}') from e

    for edge_type, edge_colmaps in file2networkmap.edges.items():
        for edge_colmap in edge_colmaps:
            try:
                extract_and_merge_x_type(
                    table, output_dir, edge_type, edge_colmap)
            except MergeDataFrameError as e:
                raise EdgeExtractError(
                    f'Failed to process > {source_file_path}. {edge_type} {edge_colmap} {str(e)}') from e


def extract_from_folder(source_folder_path: Path, output_dir: Path, config: Config) -> None:

    make_dir(output_dir)

    for folder, files_2networkmap in tqdm(config.folder2networkmap.items(), desc=f'Processing subfolders in > {str(source_folder_path).split("/")[-1]}'):
        source_folder = source_folder_path / folder
        for file, file2networkmap in files_2networkmap.items():
            for filepath in tqdm(glob.glob(str(source_folder / file)), desc=f'Files from > {folder}/{file}', leave=False):
                try:
                    source_file_path = Path(filepath)
                    extract_from_file(source_file_path,
                                      output_dir, file2networkmap)
                except ExtractError as e:
                    logging.warning(
                        f'Failed to process > {str(filepath)}. Error: {e} , type: {type(e).__name__}')
                    break

                except SchemaMismatchError as e:
                    logging.warning(
                        f'Failed to process > {str(filepath)}. Error: {e} , type: {type(e).__name__}')
                    break

                except Exception as e:
                    logging.error(
                        f'Unhandled Error, failed to process > {str(filepath)}. Error: {e}, type: {type(e).__name__}')
                    raise e

    # remove_non_trivial_duplicates_from_folder(output_dir, config)


def list_folder_files(folder: Path) -> List[Path]:
    return [f for f in folder.iterdir() if f.is_file() and f.suffix == '.parquet']


def show_folder_files_stats(folder: Path) -> None:
    files = list_folder_files(folder)

    # show row count for each file
    for file in files:
        table = pl.read_parquet(file)
        logging.info(f'{file} rows: {len(table)}')


def convert_parquet_to_csv(parquet_path: Path) -> None:

    csv_path = parquet_path.with_suffix('.csv')

    table = pl.read_parquet(parquet_path)
    table.write_csv(csv_path)


def convert_parquet_folder_contents_to_csv(folder_path: Path) -> None:
    for parquet_path in (pbar := tqdm(folder_path.glob('*.parquet'))):
        pbar.set_description(f'Converting {parquet_path}')
        convert_parquet_to_csv(parquet_path)
