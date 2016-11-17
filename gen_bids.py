import os
import argparse
import logging
import multiprocessing
import json
from converters import convert_to_bids
from utils import create_path
from datetime import datetime
from collections import OrderedDict


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
        "--mapping_dir",
        help="absolute path where generated map should be saved to",
        default=os.path.join(os.getcwd(), "mappings")
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

    parser.add_argument(
        "--scanner_meta",
        help="Add relevant DICOM metadata from scanner to the mappings file",
        action="store_true",
        default=False
    )

    settings = parser.parse_args()

    date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Configure logger
    log_fname = "bids_conversion_{}.log".format(date_str)
    log_fpath = os.path.join(settings.logs, log_fname)

    logging.basicConfig(
        filename=log_fpath,
        level=logging.DEBUG,
        format='LOG ENTRY %(asctime)s - %(levelname)s \n%(message)s \nEND LOG ENTRY\n'
    )

    print("Beginning conversion to BIDS format of data in {} directory.\n"
          "Log located in {}.".format(settings.oxygen_dir, log_fpath))

    if settings.filters:
        with open(settings.filters, "r") as filter_file:
            filters = json.load(filter_file)
    else:
        filters = None

    mapping = convert_to_bids(settings.bids_dir, settings.oxygen_dir, mapping_guide=settings.mapping_guide,
                              conversion_tool='dcm2niix', logger=logging, nthreads=settings.nthreads,
                              overwrite=settings.overwrite, filters=filters,
                              scanner_meta=settings.scanner_meta)

    print("BIDS conversion complete. Results stored in {} directory".format(settings.bids_dir))

    # Save mapping
    if not os.path.isdir(settings.mapping_dir):
        create_path(settings.mapping_dir)

    map_fpath = os.path.join(settings.mapping_dir, "bids_mapping_{}.json".format(date_str))

    with open(map_fpath, "w") as outfile:
        contents = json.dumps(mapping, sort_keys=True, indent=4, separators=(',', ': '))

        # Sort the mapping by bids subject (YES, I KNOW THIS IS NOT THE MOST ELEGANT WAY TO DO THIS)
        sorted_by_key = json.loads(contents, object_pairs_hook=OrderedDict)

        # Get the bids subject ids present
        bids_ids = sorted([sorted_by_key[key]["bids_subject"] for key in sorted_by_key.keys()])

        sorted_by_bids = OrderedDict()

        for bids_id in bids_ids:

            for subject in sorted_by_key.keys():

                if sorted_by_key[subject]["bids_subject"] == bids_id:
                    sorted_by_bids[subject] = sorted_by_key[subject]
                    break

        sorted_contents = json.dumps(sorted_by_bids, indent=4, separators=(',', ': '))

        outfile.write(sorted_contents)

    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        handler.close()
        logging.root.removeHandler(handler)
