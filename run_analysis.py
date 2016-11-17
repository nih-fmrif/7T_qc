import argparse
import os
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Semaphore
from utils import log_output, create_path
from algorithms import get_tsnr
from glob import glob
from multiprocessing import cpu_count
from collections import OrderedDict
from datetime import datetime


MAX_WORKERS = (cpu_count() * 5) // 4


class DuplicateFile(Exception):
    def __init__(self, message):
        self.message = message


if __name__ == "__main__":

    tsnr_semaphore = Semaphore(value=1)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "bids_dir",
        help="BIDS directory with input files"
    )

    parser.add_argument(
        "output_dir",
        help="output directory for analysis files",
        default=os.path.join(os.getcwd(), "tsnr")
    )

    parser.add_argument(
        "--log_dir",
        help="absolute path for log directory",
        default=os.path.join(os.getcwd(), "logs")
    )

    parser.add_argument(
        "--nthreads",
        help="Number of threads to use. Choose 0 to run sequentially. Default is (NUM_CPU_CORES * 5) // 4",
        default=MAX_WORKERS
    )

    parser.add_argument(
        "--overwrite",
        help="Overwrite existing BIDS files. NOT RECOMMENDED.",
        action="store_true",
        default=False
    )

    settings = parser.parse_args()

    if not os.path.isdir(settings.log_dir):
        create_path(settings.log_dir)

    date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Configure logger
    log_fname = "run_analysis_{}.log".format(date_str)
    log_fpath = os.path.join(settings.log_dir, log_fname)

    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        handler.close()
        logging.root.removeHandler(handler)

    logging.basicConfig(
        filename=log_fpath,
        level=logging.DEBUG,
        format='LOG ENTRY %(asctime)s - %(levelname)s \n%(message)s \nEND LOG ENTRY\n'
    )

    settings_str = "Bids directory: {}\n".format(settings.bids_dir) + \
                   "Output directory: {}\n".format(settings.output_dir) + \
                   "Log directory: {}\n".format(settings.log_dir) + \
                   "No. of Threads: {}\n".format(settings.nthreads) + \
                   "Overwrite: {}\n".format(settings.overwrite)

    log_output(settings_str, logger=logging)

    log_output("Beginning analysis...", logger=logging)

    # If analysis output directory exists, verify that it's either empty, or that overwrite is allowed.
    # Otherwise create directory.
    if os.path.isdir(settings.output_dir):

        analysis_files = glob(os.path.join(settings.output_dir, '*'))

        if analysis_files:
            if not settings.overwrite:
                raise DuplicateFile("The output directory is not empty, and overwrite is set to False. Aborting...")
            else:
                rm_files = glob(os.path.join(settings.output_dir, '*'))
                list(map(shutil.rmtree, rm_files))
    else:
        create_path(settings.output_dir)

    # Create summary file and add the header row
    summary_file = os.path.join(settings.output_dir, "TSNR_summaries.csv")
    with open(summary_file, "w") as f:
        f.write("Image,TSNR\n")
    
    # Get all the Nifti images from the BIDS directory
    nii_imgs = glob(os.path.join(settings.bids_dir, "*", "*", "*", "*.nii*"))

    analysis_results = {}

    if settings.nthreads > 0:

        futures = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for img in nii_imgs:
                out_dir = os.path.join(settings.output_dir, os.path.basename(img).split(".")[0])
                futures.append(executor.submit(get_tsnr, img, out_dir, logging, tsnr_semaphore))

        wait(futures)
        for future in futures:
            clean_fname, tsnr_val = future.result()
            analysis_results[clean_fname] = tsnr_val

    else:
        for img in nii_imgs:
            out_dir = os.path.join(settings.output_dir, os.path.basename(img).split(".")[0])
            clean_fname, tsnr_val = get_tsnr(img, out_dir, logger=logging)
            analysis_results[clean_fname] = tsnr_val

    sorted_results = OrderedDict(sorted(analysis_results.items(), key=lambda t: t[0]))

    # Write results to summary file
    with open(summary_file, "a") as f:
        for clean_fname, tsnr_val in sorted_results.items():
            f.write("{},{}\n".format(clean_fname, tsnr_val))

    log_output("Analysis complete!", logger=logging)

    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        handler.close()
        logging.root.removeHandler(handler)
