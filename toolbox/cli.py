from argparse import ArgumentParser
from pathlib import Path
import logging

from utils import ROOT_DIR


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        '--folder', help='folder where the data is stored', required=True, type=Path)
    parser.add_argument(
        '--config', help='config to convert the files, see example in configs folder', required=True, type=Path)
    parser.add_argument(
        '--output', help='output folder where the converted files will be stored', required=False, type=Path, default=ROOT_DIR / 'output_data')

    # add flag if you want to convert the output to csv as well
    parser.add_argument(
        '--csv', help='flag to convert the output to csv as well', action='store_true')

    args = parser.parse_args()
    logging.info(args)
    return args


def main():
    args = parse_args()

    print(args)


if __name__ == '__main__':
    main()
