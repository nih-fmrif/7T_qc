import os
import shutil
import multiprocessing
from subprocess import CalledProcessError, check_output, STDOUT
from glob import glob
from concurrent.futures import ThreadPoolExecutor, wait
from utils import log_output, create_path, extract_tgz, filter_series, get_scanner_meta
from threading import Semaphore


LOG_MESSAGES = {
    'success_converted':
        'Converted {} to {}\n'
        'Command:\n{}\n'
        'Return Code:\n{}\n\n',
    'output':
        'Output:\n{}\n\n',
    'dcm2niix_error':
        'Error running dcm2niix on DICOM series in {} directory.\n'
        'Command:\n{}\n'
        'Return Code:\n{}\n\n',
    'dimon_error':
        'Error running Dimon on DICOM series in {} directory.\n'
        'Command:\n{}\n'
        'Return Code:\n{}\n\n',
}


MAX_WORKERS = multiprocessing.cpu_count() * 5


class NiftyConversionFailure(Exception):
    def __init__(self, message):
        self.message = message


class DuplicateFile(Exception):
    def __init__(self, message):
        self.message = message


def dcm_to_nifti(dcm_dir, out_fname, out_dir, conversion_tool, logger=None, bids_meta=False, semaphore=None):

    if conversion_tool == 'dcm2niix':

        dcm2niix_workdir = dcm_dir

        if bids_meta:
            cmd = [
                "dcm2niix",
                "-z",
                "y",
                "-b",
                "y",
                "-f",
                out_fname,
                dcm_dir
            ]
        else:
            cmd = [
                "dcm2niix",
                "-z",
                "y",
                "-f",
                out_fname,
                dcm_dir
            ]

        try:

            result = check_output(cmd, stderr=STDOUT, cwd=dcm2niix_workdir, universal_newlines=True)

            # The following line is a hack to get the actual filename returned by the dcm2niix utility. When converting
            # the B0 dcm files, or files that specify which coil they used, or whether they contain phase information,
            # the utility appends some prefixes to the filename it saves, instead of just using
            # the specified output filename. There is no option to turn this off (and the author seemed unwilling to
            # add one). With this hack I retrieve the actual filename it used to save the file from the utility output.
            # This might break on future updates of dcm2niix
            actual_fname = \
                [s for s in ([s for s in str(result).split('\n') if "Convert" in s][0].split(" "))
                 if s[0] == '/'][0].split("/")[-1]

            # Move nifti file and json bids file to anat folder
            shutil.move(os.path.join(dcm_dir, "{}.nii.gz".format(actual_fname)),
                        os.path.join(out_dir, "{}.nii.gz".format(out_fname)))
            shutil.move(os.path.join(dcm_dir, "{}.json".format(actual_fname)),
                        os.path.join(out_dir, "{}.json".format(out_fname)))

            dcm_file = [f for f in os.listdir(dcm_dir) if ".dcm" in f][0]

            log_str = LOG_MESSAGES['success_converted'].format(os.path.join(dcm_dir, dcm_file), out_fname,
                                                               " ".join(cmd), 0)

            if result:
                log_str += LOG_MESSAGES['output'].format(result)

            log_output(log_str, logger=logger, semaphore=semaphore)

            return ("/".join(dcm2niix_workdir.split("/")[-3:]),
                    os.path.join("/".join(out_dir.split("/")[-4:]), out_fname + ".nii.gz"),
                    True)

        except CalledProcessError as e:

            log_str = LOG_MESSAGES['dcm2niix_error'].format(dcm_dir, " ".join(cmd), e.returncode)

            if e.output:
                log_str += LOG_MESSAGES['output'].format(e.output)

            log_output(log_str, level="ERROR", logger=logger, semaphore=semaphore)

            return ("/".join(dcm2niix_workdir.split("/")[-3:]),
                    os.path.join("/".join(out_dir.split("/")[-4:]), out_fname + ".nii.gz"),
                    False)

        finally:

            # Clean up temporary files
            tmp_files = glob(os.path.join(dcm2niix_workdir, "*.nii.gz"))
            tmp_files.extend(glob(os.path.join(dcm2niix_workdir, "*.json")))

            if tmp_files:
                list(map(os.remove, tmp_files))

    elif conversion_tool == 'dimon':

        dimon_workdir = dcm_dir

        # IMPLEMENT GENERATION OF BIDS METADATA FILES WHEN USING DIMON FOR CONVERSION OF DCM FILES

        cmd = [
            "Dimon",
            "-infile_pattern",
            os.path.join(dcm_dir, "*.dcm"),
            "-gert_create_dataset",
            "-gert_quit_on_err",
            "-gert_to3d_prefix",
            "{}.nii.gz".format(out_fname)
        ]

        dimon_env = os.environ.copy()
        dimon_env['AFNI_TO3D_OUTLIERS'] = 'No'

        try:

            result = check_output(cmd, stderr=STDOUT, env=dimon_env, cwd=dimon_workdir, universal_newlines=True)

            # Check the contents of stdout for the -quit_on_err flag because to3d returns a success code
            # even if it terminates because the -quit_on_err flag was thrown
            if "to3d kept from going into interactive mode by option -quit_on_err" in result:

                log_str = LOG_MESSAGES['dimon_error'].format(dcm_dir, " ".join(cmd), 0)

                if result:
                    log_str += LOG_MESSAGES['output'].format(result)

                log_output(log_str, level="ERROR", logger=logger, semaphore=semaphore)

                return ("/".join(dimon_workdir.split("/")[-3:]),
                        os.path.join("/".join(out_dir.split("/")[-4:]), out_fname + ".nii.gz"),
                        False)

            shutil.move(os.path.join(dimon_workdir, "{}.nii.gz".format(out_fname)),
                        os.path.join(out_dir, "{}.nii.gz".format(out_fname)))

            dcm_file = [f for f in os.listdir(dcm_dir) if ".dcm" in f][0]

            log_str = LOG_MESSAGES['success_converted'].format(os.path.join(dcm_dir, dcm_file), out_fname,
                                                               " ".join(cmd), 0)

            if result:
                log_str += LOG_MESSAGES['output'].format(result)

            log_output(log_str, logger=logger, semaphore=semaphore)

            return ("/".join(dimon_workdir.split("/")[-3:]),
                    os.path.join("/".join(out_dir.split("/")[-4:]), out_fname + ".nii.gz"),
                    True)

        except CalledProcessError as e:

            log_str = LOG_MESSAGES['dimon_error'].format(dcm_dir, " ".join(cmd), e.returncode)

            if e.output:
                log_str += LOG_MESSAGES['output'].format(e.output)

            log_output(log_str, level="ERROR", logger=logger, semaphore=semaphore)

            return ("/".join(dimon_workdir.split("/")[-3:]),
                    os.path.join("/".join(out_dir.split("/")[-4:]), out_fname + ".nii.gz"),
                    False)

        finally:

            # Clean up temporary files
            tmp_files = glob(os.path.join(dimon_workdir, "GERT_Reco_dicom*"))
            tmp_files.extend(glob(os.path.join(dimon_workdir, "dimon.files.run.*")))

            if tmp_files:
                list(map(os.remove, tmp_files))

    else:

        raise NiftyConversionFailure("Tool Error: {} is not a supported conversion tool. Please select 'dcm2niix' or "
                                     "'dimon'".format(conversion_tool))


def convert_to_bids(bids_dir, oxygen_dir, mapping_guide=None, conversion_tool='dcm2niix', logger=None,
                    nthreads=MAX_WORKERS, overwrite=False, filters=None, scanner_meta=False):

    if nthreads > 0:
        thread_semaphore = Semaphore(value=1)
    else:
        thread_semaphore = None

    # If BIDS directory exists, verify that it's either empty, or that overwrite is allowed. Otherwise create directory.
    if os.path.isdir(bids_dir):

        bids_files = glob(os.path.join(bids_dir, '*'))

        if bids_files:
            if not overwrite:
                raise DuplicateFile("The BIDS directory is not empty, and overwrite is set to False. Aborting...")
            else:
                rm_files = glob(os.path.join(bids_dir, '*'))
                list(map(shutil.rmtree, rm_files))
    else:
        create_path(bids_dir)

    # Uncompress any compressed Oxygen DICOM files
    raw_files = os.path.join(oxygen_dir, '*')

    # Check if there are compressed oxygen files, and if so, uncompress them
    compressed_files = [d for d in glob(raw_files) if os.path.isfile(d)]

    log_output("Extracting compressed files...", logger=logger)

    if nthreads > 0:   # Run in multiple threads

        futures = []

        with ThreadPoolExecutor(max_workers=nthreads) as executor:

            for f in compressed_files:

                futures.append(executor.submit(extract_tgz, f, oxygen_dir, logger, thread_semaphore))

        wait(futures)

    else:   # Run sequentially
        for f in compressed_files:
            extract_tgz(f, oxygen_dir, logger=logger)

    log_output("Compressed file extractions complete.", logger=logger)

    # Now we can get a list of uncompressed directories
    uncompressed_files = [d for d in glob(raw_files) if os.path.isdir(d)]

    mapping = {}

    # If a BIDS mapping has not be provided to guide the conversion process, attempt to generate mapping from
    # available information.
    if not mapping_guide:

        subject_counter = 1

        mapping = {}

        for unc_file in uncompressed_files:

            subject_id = unc_file.split("/")[-1].split("-")[-1]

            if subject_id not in mapping.keys():

                mapping[subject_id] = {
                    "bids_subject": "{:0>4d}".format(subject_counter),
                    "sessions": {}
                }

                subject_counter += 1

            session_dirs = [d for d in glob(os.path.join(unc_file, '*')) if os.path.isdir(d)]

            session_counter = 1

            for ses_dir in session_dirs:

                session_id = ses_dir.split("/")[-1]

                mapping[subject_id]["sessions"][session_id] = {
                    "bids_session": "{:0>4d}".format(session_counter),
                    "oxygen_file": "{}-{}-DICOM.tgz".format(ses_dir.split("/")[-2], ses_dir.split("/")[-1]),
                    "scans": {}
                }

                session_counter += 1

                scan_dirs = [d for d in glob(os.path.join(ses_dir, '*')) if os.path.isdir(d) and "mr_" in d]

                scan_counter = 1

                for sc_dir in scan_dirs:

                    scan_id = sc_dir.split("/")[-1]

                    # Filter this series directory
                    if filter_series(sc_dir, filters=filters, logger=logger):
                        continue

                    mapping[subject_id]["sessions"][session_id]["scans"][scan_id] = {
                        "series_dir": "/".join(sc_dir.split("/")[-3:]),
                        "bids_fpath": "",
                        "conversion_status": False,
                        "meta": {
                            "type": "func",
                            "modality": "bold",
                            "description": "task-fmri",
                            "run": "{:0>4d}".format(scan_counter)
                        }
                    }

                    if scanner_meta:
                        meta = get_scanner_meta(sc_dir)
                        mapping[subject_id]["sessions"][session_id]["scans"][scan_id]["scanner_meta"] = meta

                    scan_counter += 1

    # Mapping has been generated
    # Iterate through the mapping to create execution list to be split into threads
    exec_list = []

    for subject in mapping.keys():
        for session in mapping[subject]["sessions"].keys():
            for scan in mapping[subject]["sessions"][session]["scans"].keys():

                series_dir = os.path.join(oxygen_dir,
                                          mapping[subject]["sessions"][session]["scans"][scan]["series_dir"])
                bids_subject = "sub-{}".format(mapping[subject]["bids_subject"])
                bids_session = "ses-{}".format(mapping[subject]["sessions"][session]["bids_session"])
                bids_desc = mapping[subject]["sessions"][session]["scans"][scan]["meta"]["description"]
                bids_type = mapping[subject]["sessions"][session]["scans"][scan]["meta"]["type"]
                bids_modality = mapping[subject]["sessions"][session]["scans"][scan]["meta"]["modality"]
                bids_run = "run-{}".format(mapping[subject]["sessions"][session]["scans"][scan]["meta"]["run"])
                bids_fname = "{}_{}_{}_{}".format(bids_subject, bids_session, bids_desc, bids_run)

                if bids_modality:
                    bids_fname += "_{}".format(bids_modality)

                bids_fname = "{}.nii.gz".format(bids_fname)

                bids_fpath = os.path.join(bids_dir, bids_subject, bids_session, bids_type, bids_fname)

                exec_list.append((series_dir, bids_fpath))

    # Iterate through executable list and convert to nifti
    if nthreads > 0:    # Run in multiple threads

        futures = []

        with ThreadPoolExecutor(max_workers=nthreads) as executor:

            for dcm_dir, bids_fpath in exec_list:

                out_bdir = "/".join(bids_fpath.split("/")[:-1])
                if not os.path.isdir(out_bdir):
                    create_path(out_bdir)

                out_fname = bids_fpath.split("/")[-1].split(".")[0]

                futures.append(executor.submit(dcm_to_nifti, dcm_dir, out_fname, out_bdir,
                                               conversion_tool=conversion_tool, bids_meta=True, logger=logger,
                                               semaphore=thread_semaphore))
                ## FOR TESTING
                # break
                #######

            wait(futures)

            for future in futures:
                series_dir, bids_fpath, success = future.result()

                subject = series_dir.split("/")[0].split("-")[1]
                session = series_dir.split("/")[1]
                scan = series_dir.split("/")[2]

                if success:
                    mapping[subject]["sessions"][session]["scans"][scan]["bids_fpath"] = bids_fpath
                    mapping[subject]["sessions"][session]["scans"][scan]["conversion_status"] = True

    else:   # Run sequentially

        for dcm_dir, bids_fpath in exec_list:

            out_bdir = "/".join(bids_fpath.split("/")[:-1])
            if not os.path.isdir(out_bdir):
                create_path(out_bdir)

            out_fname = bids_fpath.split("/")[-1].split(".")[0]

            series_dir, bids_fpath, success = dcm_to_nifti(dcm_dir, out_fname, out_bdir, conversion_tool='dcm2niix',
                                                           bids_meta=True, logger=logger)

            subject = series_dir.split("/")[0].split("-")[1]
            session = series_dir.split("/")[1]
            scan = series_dir.split("/")[2]

            if success:
                mapping[subject]["sessions"][session]["scans"][scan]["bids_fpath"] = bids_fpath
                mapping[subject]["sessions"][session]["scans"][scan]["conversion_status"] = True

    return mapping
