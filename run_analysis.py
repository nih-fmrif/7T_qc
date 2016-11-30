import argparse
import os
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Semaphore
from utils import log_output, create_path
from workflows import seven_tesla_wf
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
        default=MAX_WORKERS,
        type=int
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
    summary_file = os.path.join(settings.output_dir, "Statistics.csv")
    with open(summary_file, "w") as f:
        f.write("Image,Mean tSNR,Pre-reg FWHM X,Pre-reg FWHM Y,Pre-reg FWHM Z,Pre-reg FWHM,"
                "Post-reg FWHM X,Post-reg FWHM Y,Post-reg FWHM Z,Post-reg FWHM,Mean FD (mm),"
                "No. FD > 0.2mm,% FD > 0.2mm\n")
    
    # Get all the Nifti images from the BIDS directory
    nii_imgs = glob(os.path.join(settings.bids_dir, "*", "*", "*", "*.nii*"))

    analysis_results = {}

    if settings.nthreads > 0:

        futures = []
        with ThreadPoolExecutor(max_workers=settings.nthreads) as executor:
            for img in nii_imgs:
                futures.append(executor.submit(seven_tesla_wf, img, settings.output_dir, logging, tsnr_semaphore))

        wait(futures)
        for future in futures:
            clean_fname, statistics = future.result()
            analysis_results[clean_fname] = statistics

    else:
        for img in nii_imgs:
            clean_fname, statistics = seven_tesla_wf(img, settings.output_dir, logger=logging)
            analysis_results[clean_fname] = statistics

    sorted_results = OrderedDict(sorted(analysis_results.items(), key=lambda t: t[0]))

    # Write results to summary file
    with open(summary_file, "a") as f:
        for clean_fname, statistics in sorted_results.items():

            if statistics is None:
                f.write("{},None,None,None,None,None,None,None,None,None,None,None,None\n".format(clean_fname))
            else:
                f.write("{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format(
                    clean_fname,
                    statistics['tsnr_val'],
                    statistics['prereg_fwhm_x'],
                    statistics['prereg_fwhm_y'],
                    statistics['prereg_fwhm_z'],
                    statistics['prereg_fwhm_combined'],
                    statistics['postreg_fwhm_x'],
                    statistics['postreg_fwhm_y'],
                    statistics['postreg_fwhm_z'],
                    statistics['postreg_fwhm_combined'],
                    statistics['mean_fd'],
                    statistics['num_fd_above_cutoff'],
                    statistics['perc_fd_above_cutoff']
                ))

    log_output("Analysis complete!", logger=logging)

    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        handler.close()
        logging.root.removeHandler(handler)
