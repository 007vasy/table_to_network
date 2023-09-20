from argparse import ArgumentParser
from pathlib import Path
import logging

from utils import (
    convert_parquet_folder_contents_to_csv,
    show_folder_files_stats
)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        '--folder', help='folder where the data is stored', required=True, type=Path)

    args = parser.parse_args()
    logging.info(args)
    return args


def main():
    args = parse_args()

    convert_parquet_folder_contents_to_csv(args.folder)

    show_folder_files_stats(args.folder)


if __name__ == '__main__':
    main()
