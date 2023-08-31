import polars as pl
from polars.testing import assert_frame_equal

from utils import (
    parse_config,
    Config,
    File2NetworkMap,
    Node2ColMap,
    Edge2ColMap,
    extract_node_type_from_table,
    extract_edge_type_from_table
)
import unittest

class TestConfig(unittest.TestCase):
    def test_config_parse(self):
        raw_config = {
            'bq_exports': {
                'output_*.json':{
                    'nodes': {
                        'ADDR':{
                            'id': 'id',
                            'attributes': {
                                'name': 'name',
                                'type': 'type'
                            }
                        }
                    },
                    'edges': {
                        'OWNS':{
                            'source': 'source',
                            'target': 'target',
                            'attributes': {
                                'type': 'type'
                            }
                        }
                    }
                }
            }
        }

        desired_config:Config = Config(
            folder2networkmap={
                'bq_exports': {
                    'output_*.json':File2NetworkMap(
                        nodes={
                            'ADDR':Node2ColMap(
                                id='id',
                                attributes={
                                    'name': 'name',
                                    'type': 'type'
                                }
                            )
                        },
                        edges={
                            'OWNS':Edge2ColMap(
                                source='source',
                                target='target',
                                attributes={
                                    'type': 'type'
                                }
                            )
                        }
                    )
                }
            }
        )

        actual_config = parse_config(raw_config)
        # print("/n/n/")
        # print(actual_config)
        # print(desired_config)

        self.assertEqual(actual_config, desired_config)

class TestNodeFileGen(unittest.TestCase):
    def test_singular_node_gen(self):
        COL_ADDRESS = '_address'
        ATTRIBUTE_ADDRESS = 'address'
        raw_table = pl.DataFrame({
            COL_ADDRESS:['Ox000', 'Ox001', 'Ox002', 'Ox000'],

        })

        desired_table = pl.DataFrame({
            'id':['Ox000', 'Ox001', 'Ox002'],
            ATTRIBUTE_ADDRESS:['Ox000', 'Ox001', 'Ox002'],
        })

        nodeColMap = Node2ColMap(
            id=COL_ADDRESS,
            attributes={
                ATTRIBUTE_ADDRESS: COL_ADDRESS,
            }
        )


        actual_table = extract_node_type_from_table(raw_table, nodeColMap)

        assert_frame_equal(desired_table.sort('id'), actual_table.sort('id'))


    def test_multiple_node_gen(self):
        pass

class TestEdgeFileGen(unittest.TestCase):
    def test_singular_edge_gen(self):
        raw_table = pl.DataFrame({
            '_source':['Ox000', 'Ox001', 'Ox002', 'Ox000'],
            '_target':['Ox001', 'Ox002', 'Ox003', 'Ox001'],
            '_type':['OWNS', 'OWNS', 'OWNS', 'OWNS'],
        })

        desired_table = pl.DataFrame({
            'source':['Ox000', 'Ox001', 'Ox002'],
            'target':['Ox001', 'Ox002', 'Ox003'],
            'type':['OWNS', 'OWNS', 'OWNS'],
        })

        edgeColMap = Edge2ColMap(
            source='_source',
            target='_target',
            attributes={
                'type': '_type'
            }
        )

        actual_table = extract_edge_type_from_table(raw_table, edgeColMap)

        assert_frame_equal(desired_table.sort(['source', 'target']), actual_table.sort(['source', 'target']))

    def test_multiple_edge_gen(self):
        pass
