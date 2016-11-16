import os
import errno
import tarfile
import dicom


def log_output(log_str, level="INFO", logger=None, semaphore=None):

    if semaphore:
        semaphore.acquire()

    if logger:

        if level == 'DEBUG':
            logger.debug(log_str)
        elif level == 'WARNING':
            logger.warning(log_str)
        elif level == 'CRITICAL':
            logger.critical(log_str)
        elif level == 'ERROR':
            logger.error(log_str)
        elif level == 'INFO':
            logger.info(log_str)

    else:
        print(log_str)

    if semaphore:
        semaphore.release()


def create_path(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def extract_tgz(fpath, out_path='.', logger=None, semaphore=None):

    if not tarfile.is_tarfile(fpath):
        raise tarfile.TarError("{} is not a valid tar/gzip file.".format(fpath))

    tar = tarfile.open(fpath, "r:gz")
    scans_folder = tar.next().name
    tar.extractall(path=out_path)
    tar.close()

    extracted_dir = "{}"

    if out_path == ".":
        extracted_dir = extracted_dir.format(os.path.join(os.getcwd(), scans_folder))
    else:
        extracted_dir = extracted_dir.format(os.path.join(out_path, scans_folder))

    log_output("Extracted file {} to {} directory.".format(fpath, extracted_dir), logger=logger, semaphore=semaphore)

    return extracted_dir


def filter_series(scan_dir, filters=None, logger=None):

    if filters:

        dcm_files = [f for f in os.listdir(scan_dir) if ".dcm" in f]

        if not dcm_files:
            log_output("No DICOM files found in {} directory. Skipping...".format(scan_dir), logger=logger)
            return False

        for scan_filter in filters.keys():

            if scan_filter == "sequences":

                # Pick an arbitrary DICOM file from the current scan folder, and retrieve the sequence
                # name
                curr_dcm = dicom.read_file(os.path.join(scan_dir, dcm_files[0]))
                seq_name = curr_dcm.SequenceName.strip()

                if seq_name not in filters[scan_filter]:

                    log_output("The series {} does not contains a sequence within the allowed list of sequences. "
                               "Skipping...".format(scan_dir))
                    return True

    return False
