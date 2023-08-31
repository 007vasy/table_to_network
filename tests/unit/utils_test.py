from utils import (
    parse_config,
    Config,
    File2NetworkMap,
    Node2ColMap,
    Edge2ColMap
)
import unittest

class TestConfig(unittest.TestCase):
    def test_config_parse(self):
        raw_config = {
            'bq_exports': {
                'output_*.json':{
                    'nodes': {
                        'id': 'id',
                        'attributes': {
                            'name': 'name',
                            'type': 'type'
                        }
                    },
                    'edges': {
                        'source': 'source',
                        'target': 'target',
                        'attributes': {
                            'type': 'type'
                        }
                    }
                }
            }
        }

        desired_config:Config = Config(
            folder2networkmap={
                'bq_exports': {
                    'output_*.json':File2NetworkMap(
                        nodes=Node2ColMap(
                            id='id',
                            attributes={
                                'name': 'name',
                                'type': 'type'
                            }
                        ),
                        edges=Edge2ColMap(
                            source='source',
                            target='target',
                            attributes={
                                'type': 'type'
                            }
                        )
                    )
                }
            }
        )

        actual_config = parse_config(raw_config)

        self.assertEqual(actual_config, desired_config)

class TestNodeFileGen(unittest.TestCase):
    def test_singular_node_gen(self):
        pass

    def test_multiple_node_gen(self):
        pass