import os
from subprocess import CalledProcessError, check_output, STDOUT
from utils import log_output, create_path
from algorithms import calc_tsnr, parse_fwhm, fd_jenkinson, extract_fd_results
from collections import OrderedDict
from glob import glob
from string import ascii_lowercase


LOG_MESSAGES = {
    "success": "Command:\n{}\nReturn Code:\n{}\n",
    "output": "Output:\n{}\n",
    "error": "Error running {}.\nCommand:\n{}\nReturn Code:\n\{}\n"
}


def seven_tesla_wf(in_file, out_dir, logger=None, semaphore=None):

    if ".nii" in in_file or ".nii.gz" in in_file:
        clean_fname = os.path.basename(in_file).split(".")[0]
    else:
        raise ValueError("Files must be in Nifti (.nii) or compressed Nifti (.nii.gz) formats.")

    cwd = os.path.join(out_dir, clean_fname)

    if not os.path.isdir(cwd):
        create_path(cwd)

    despike_fname = "{}_despike".format(os.path.join(cwd, clean_fname))

    despike = [
        "3dDespike",
        "-overwrite",
        "-prefix",
        "{}.nii.gz".format(despike_fname),
        "{}".format(in_file)
    ]

    tshift_fname = "{}_tshift".format(despike_fname)

    tshift = [
        "3dTshift",
        "-overwrite",
        "-prefix",
        "{}.nii.gz".format(tshift_fname),
        "{}.nii.gz".format(despike_fname)
    ]

    prereg_fname = "{}_prereg_fwhm.out".format(tshift_fname)

    prereg_fwhm = [
        "3dFWHMx",
        "-input",
        "{}.nii.gz".format(tshift_fname),
        "-detrend",
        "1",
        "-combine",
    ]

    oned_file = "{}.1D".format(tshift_fname)
    oned_matrix = "{}.aff12.1D".format(tshift_fname)
    max_disp = "{}_md.1D".format(tshift_fname)
    volreg_fname = "{}_volreg".format(tshift_fname)

    volreg = [
        "3dvolreg",
        "-overwrite",
        "-twopass",
        "-cubic",
        "-base",
        "3",
        "-zpad",
        "4",
        "-1Dfile",
        "{}".format(oned_file),
        "-maxdisp1D",
        "{}".format(max_disp),
        "-1Dmatrix_save",
        "{}".format(oned_matrix),
        "-prefix",
        "{}.nii.gz".format(volreg_fname),
        "{}.nii.gz".format(tshift_fname)
    ]

    postreg_fname = "{}_postreg_fwhm.out".format(volreg_fname)

    postreg_fwhm = [
        "3dFWHMx",
        "-input",
        "{}.nii.gz".format(volreg_fname),
        "-detrend",
        "1",
        "-combine",
    ]

    epi_mask_fname = "{}_mask".format(volreg_fname)

    epi_mask = [
        "3dAutomask",
        "-dilate",
        "1",
        "-prefix",
        "{}.nii.gz".format(epi_mask_fname),
        "{}.nii.gz".format(volreg_fname)
    ]

    mean_fname = "{}_mean".format(volreg_fname)

    mean = [
        "3dTstat",
        "-overwrite",
        "-mean",
        "-prefix",
        "{}.nii.gz".format(mean_fname),
        "{}.nii.gz".format(volreg_fname)
    ]

    detrend_fname = "{}_detrend".format(volreg_fname)

    detrend = [
        "3dDetrend",
        "-overwrite",
        "-polort",
        "1",
        "-prefix",
        "{}.nii.gz".format(detrend_fname),
        "{}.nii.gz".format(volreg_fname)
    ]

    detrend_with_mean_fname = "{}_detrend_with_mean".format(volreg_fname)

    detrend_with_mean = [
        "3dcalc",
        "-overwrite",
        "-a",
        "{}.nii.gz".format(mean_fname),
        "-b",
        "{}.nii.gz".format(detrend_fname),
        "-expr",
        "a+b",
        "-prefix",
        "{}.nii.gz".format(detrend_with_mean_fname)
    ]

    workflow = [
        despike,
        tshift,
        prereg_fwhm,
        volreg,
        postreg_fwhm,
        epi_mask,
        mean,
        detrend,
        detrend_with_mean
    ]

    wf_success = True

    for cmd in workflow:

        if not wf_success:
            break

        try:
            result = check_output(cmd, cwd=cwd, stderr=STDOUT, universal_newlines=True)

            # The 3dFWHMx command outputs to stdout, capture this into a file
            if "3dFWHMx" in cmd:

                outfname = ""

                if "{}.nii.gz".format(tshift_fname) in cmd:
                    outfname = prereg_fname
                elif "{}.nii.gz".format(volreg_fname) in cmd:
                    outfname = postreg_fname

                if outfname:
                    with open(outfname, "w") as outfile:
                        outfile.write(result)

            log_str = LOG_MESSAGES["success"].format(" ".join(cmd), 0)

            if result:
                log_str += LOG_MESSAGES["output"].format(result)

            log_output(log_str, logger=logger, semaphore=semaphore)

        except CalledProcessError as e:

            log_str = LOG_MESSAGES["error"].format(cmd[0], " ".join(cmd), e.returncode)

            if e.output:
                log_str += LOG_MESSAGES["output"].format(e.output)

            log_output(log_str, logger=logger, semaphore=semaphore)

            wf_success = False

    if wf_success:

        # Compute the TSNR image and the mean TSNR value
        tsnr_fname = "{}_TSNR".format(os.path.join(cwd, clean_fname))
        tsnr_infile = "{}.nii.gz".format(detrend_with_mean_fname)
        epi_mask = "{}.nii.gz".format(os.path.join(cwd, epi_mask_fname))

        tsnr_val = calc_tsnr(tsnr_fname, tsnr_infile, epi_mask)

        # Calculate the FWHM of the dataset before and after registration (using linear detrending)
        prereg_fname = os.path.join(cwd, prereg_fname)
        postreg_fname = os.path.join(cwd, postreg_fname)

        pre_fwhm_x, pre_fwhm_y, pre_fwhm_z, pre_fwhm_combined = parse_fwhm(prereg_fname)
        post_fwhm_x, post_fwhm_y, post_fwhm_z, post_fwhm_combined = parse_fwhm(postreg_fname)

        # Calculate the framewise displacement
        fd_fname = os.path.join(cwd, "{}_fd.txt".format(clean_fname))
        fd_res = fd_jenkinson(os.path.join(cwd, oned_matrix), out_file=fd_fname)

        # Parse fd results
        mean_fd, num_above_cutoff, perc_above_cutoff = extract_fd_results(fd_res, cutoff=0.2)

        statistics = OrderedDict({
            'tsnr_val': tsnr_val,
            'prereg_fwhm_x': pre_fwhm_x,
            'prereg_fwhm_y': pre_fwhm_y,
            'prereg_fwhm_z': pre_fwhm_z,
            'prereg_fwhm_combined': pre_fwhm_combined,
            'postreg_fwhm_x': post_fwhm_x,
            'postreg_fwhm_y': post_fwhm_y,
            'postreg_fwhm_z': post_fwhm_z,
            'postreg_fwhm_combined': post_fwhm_combined,
            'mean_fd': mean_fd,
            'num_fd_above_cutoff': num_above_cutoff,
            'perc_fd_above_cutoff': perc_above_cutoff
        })

        return clean_fname, statistics

    return clean_fname, None


def _register_anat(base_img, img, out_dir):

    img_name = os.path.basename(img)
    if ".nii.gz" in img:
        img_name = img_name[:-7]
    elif ".nii" in img:
        img_name = img_name[:-4]

    volreg_fname = os.path.join(out_dir, "{}_volreg.nii.gz".format(img_name))

    volreg = [
        "3dvolreg",
        "-overwrite",
        "-twopass",
        "-heptic",
        "-base",
        "{}".format(base_img),
        "-prefix",
        "{}".format(volreg_fname),
        "{}".format(img)
    ]

    return volreg


def anat_average_wf(session_dir, out_dir, logger=None, semaphore=None):

    base_img = glob(os.path.join(session_dir, "*run-01_T1w.nii*"))[0]
    additional_imgs = [img for img in glob(os.path.join(session_dir, "*.nii*")) if "run-01_T1w" not in img]

    wf = []

    for img in additional_imgs:
        wf.append(_register_anat(base_img, img, out_dir))

    wf_success = True

    for cmd in wf:

        if not wf_success:
            break

        try:
            result = check_output(cmd, cwd=session_dir, stderr=STDOUT, universal_newlines=True)

            log_str = LOG_MESSAGES["success"].format(" ".join(cmd), 0)

            if result:
                log_str += LOG_MESSAGES["output"].format(result)

            log_output(log_str, logger=logger, semaphore=semaphore)

        except CalledProcessError as e:

            log_str = LOG_MESSAGES["error"].format(cmd[0], " ".join(cmd), e.returncode)

            if e.output:
                log_str += LOG_MESSAGES["output"].format(e.output)

            log_output(log_str, logger=logger, semaphore=semaphore)

            wf_success = False

    if wf_success:

        alphabet = list(ascii_lowercase)

        calc_cmd = [
            "3dcalc"
        ]
        used_letters = []
        volreg_imgs = glob(os.path.join(out_dir, "*_volreg.nii.gz"))
        volreg_imgs.insert(0, base_img)

        for img in volreg_imgs:
            curr_letter = alphabet.pop(0)
            curr_params = [
                "-{}".format(curr_letter),
                "{}".format(img)
            ]
            calc_cmd.extend(curr_params)
            used_letters.append(curr_letter)

        expr_string = "({})/{}".format("+".join(used_letters), len(used_letters))
        expr = [
            "-expr",
            "{}".format(expr_string),
        ]

        calc_cmd.extend(expr)

        calc_name = "_".join(os.path.basename(base_img).split("_")[:2])

        calc_cmd.extend([
            "-prefix",
            "{}_anat_avg.nii.gz".format(os.path.join(out_dir, calc_name))
        ])

        try:
            result = check_output(calc_cmd, cwd=session_dir, stderr=STDOUT, universal_newlines=True)

            log_str = LOG_MESSAGES["success"].format(" ".join(calc_cmd), 0)

            if result:
                log_str += LOG_MESSAGES["output"].format(result)

            log_output(log_str, logger=logger, semaphore=semaphore)

        except CalledProcessError as e:

            log_str = LOG_MESSAGES["error"].format(calc_cmd[0], " ".join(calc_cmd), e.returncode)

            if e.output:
                log_str += LOG_MESSAGES["output"].format(e.output)

            log_output(log_str, logger=logger, semaphore=semaphore)

            return False

        return True

    return False


