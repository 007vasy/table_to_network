from argparse import ArgumentParser
from pathlib import Path
import logging

from utils import dummy


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        '--folder', help='folder where the data is stored', required=True, type=Path)
    parser.add_argument(
        '--config', help='config to convert the files, see example in configs folder', required=True, type=Path)

    args = parser.parse_args()
    logging.info(args)
    return args


def main():
    args = parse_args()
    dummy(1)
    print(args)


if __name__ == '__main__':
    main()
