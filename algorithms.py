import nibabel as nb
import numpy as np
from nipype.algorithms.confounds import TSNR


def calc_tsnr(fname, in_file, epi_mask):

    tsnr = TSNR()
    tsnr.inputs.in_file = in_file
    tsnr.inputs.tsnr_file = "{}.nii.gz".format(fname)
    tsnr.inputs.mean_file = "{}_mean.nii.gz".format(fname)
    tsnr.inputs.stddev_file = "{}_stddev.nii.gz".format(fname)
    tsnr.run()

    # FROM MRIQC
    # Get EPI data (with mc done) and get it ready
    msknii = nb.load(epi_mask)
    mskdata = np.nan_to_num(msknii.get_data())
    mskdata = mskdata.astype(np.uint8)
    mskdata[mskdata < 0] = 0
    mskdata[mskdata > 0] = 1

    tsnr_data = nb.load(fname).get_data()
    tsnr_val = float(np.median(tsnr_data[mskdata > 0]))

    return tsnr_val
