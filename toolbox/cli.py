from argparse import ArgumentParser
from pathlib import Path
import logging

from utils import (
    ROOT_DIR,
    Config,
    get_config,
    extract_from_folder,
    show_folder_files
)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        '--folder', help='folder where the data is stored', required=True, type=Path)
    parser.add_argument(
        '--config', help='config to convert the files, see example in configs folder', required=True, type=Path)
    parser.add_argument(
        '--output', help='output folder where the converted files will be stored', required=False, type=Path, default=ROOT_DIR / 'output_data')

    args = parser.parse_args()
    logging.info(args)
    return args


def main():
    args = parse_args()

    config: Config = get_config(args.config)

    extract_from_folder(args.folder, args.output, config.folder2networkmap)

    show_folder_files(args.output)


if __name__ == '__main__':
    main()
