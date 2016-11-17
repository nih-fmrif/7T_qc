import argparse
import os
import logging
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Semaphore
from utils import log_output, create_path
from algorithms import get_tsnr
from glob import glob
from multiprocessing import cpu_count
from collections import OrderedDict
from datetime import datetime


MAX_WORKERS = cpu_count() * 5


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
        help="Number of threads to use. Choose 0 to run sequentially",
        default=MAX_WORKERS
    )

    settings = parser.parse_args()

    date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Configure logger
    log_fname = "get_tsnr_{}.log".format(date_str)
    log_fpath = os.path.join(settings.log_dir, log_fname)

    logging.basicConfig(
        filename=log_fpath,
        level=logging.DEBUG,
        format='LOG ENTRY %(asctime)s - %(levelname)s \n%(message)s \nEND LOG ENTRY\n'
    )

    log_output("Beginning analysis...", logger=logging)

    if not os.path.isdir(settings.output_dir):
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



