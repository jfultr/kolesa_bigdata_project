import os.path
import argparse
from kolesa_parser import run_parser
from predict import get_predict


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        description="Enter full or relative path for file with data. "
                    "If file with data not exists program will create new dataset "
                    "and name it with the entered name")
    arg_parser.add_argument('path', metavar='p', type=str, help='path to file with data')

    args = arg_parser.parse_args()
    path_to_file = args.path
    if ':' not in path_to_file:
        path_to_file = os.path.dirname(__file__) + '/' + path_to_file

    # if script runs without path to CSV file, data will be collected from website
    if not os.path.exists(path_to_file):
        if input(f'Program starting scraping data from kolesa.kz'
                 f'it can take a long time and not so safe'
                 f'Enter "y" to continue') == 'y':
            run_parser(path_to_file)
        else:
            exit()
    get_predict(path_to_file)
