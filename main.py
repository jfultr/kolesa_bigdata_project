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
    arg_parser.add_argument('--update', metavar='u', type=bool, nargs='?', const=True,
                            default=False, help='update dataset')

    args = arg_parser.parse_args()
    path_to_file = args.path
    update = bool(args.update)
    if ':' not in path_to_file:
        path_to_file = os.path.dirname(__file__) + '/' + path_to_file

    # if script runs without path to CSV file, data will be collected from website
    if not os.path.exists(path_to_file) or update:
        if input(f'Program starting scraping data from kolesa.kz\n'
                 f'it can take a long time and not so safe\n'
                 f'Enter "y" to continue: ') == 'y':
            run_parser(path_to_file)
        else:
            exit()
    # get_predict(path_to_file, 'RFR')
