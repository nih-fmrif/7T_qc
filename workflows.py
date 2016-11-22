import os
from subprocess import CalledProcessError, check_output, STDOUT
from utils import log_output, create_path
from algorithms import calc_tsnr, parse_fwhm, fd_jenkinson
from collections import OrderedDict


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
        res_file = fd_jenkinson(os.path.join(cwd, oned_matrix), out_file=fd_fname)

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
        })

        return clean_fname, statistics

    return clean_fname, None
