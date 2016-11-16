import os
import argparse
import logging
from converters import convert_to_bids
import multiprocessing
from datetime import datetime


MAX_WORKERS = multiprocessing.cpu_count() * 5


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "bids_dir",
        help="absolute path to bids directory for output"
    )

    parser.add_argument(
        "oxygen_dir",
        help="absolute path for directory containing oxygen files",
    )

    parser.add_argument(
        "--mapping_guide",
        help="absolute path to mapping guide for BIDS conversion",
        default=None
    )

    parser.add_argument(
        "--nthreads",
        help="number of threads to use when running this script. Use 0 for sequential run.",
        default=MAX_WORKERS
    )

    parser.add_argument(
        "--overwrite",
        help="Overwrite existing BIDS files. NOT RECOMMENDED.",
        action="store_true",
        default=False
    )

    parser.add_argument(
        "--filters",
        help="absolute path to json file containing series filters",
        default=None
    )

    parser.add_argument(
        "--logs",
        help="absolute path to log directory",
        default=os.path.join(os.getcwd(), "logs")
    )

    settings = parser.parse_args()

    # Configure logger
    log_fname = "bids_conversion_{}.log".format(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    log_fpath = os.path.join(settings.logs, log_fname)

    logging.basicConfig(
        filename=log_fpath,
        level=logging.DEBUG,
        format='LOG ENTRY %(asctime)s - %(levelname)s \n%(message)s \nEND LOG ENTRY\n'
    )

    print("Beginning conversion to BIDS format of data in {} directory.\n"
          "Log located in {}.".format(settings.oxygen_dir, log_fpath))

    convert_to_bids(settings.bids_dir, settings.oxygen_dir, mapping_guide=settings.mapping_guide,
                    conversion_tool='dcm2niix', logger=logging, nthreads=settings.nthreads,
                    overwrite=settings.overwrite, filters=settings.filters)

    print("BIDS conversion complete. Results stored in {} directory".format(settings.bids_dir))

    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        handler.close()
        logging.root.removeHandler(handler)
