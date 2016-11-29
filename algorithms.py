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

    tsnr_data = nb.load("{}.nii.gz".format(fname)).get_data()
    tsnr_val = float(np.median(tsnr_data[mskdata > 0]))

    return tsnr_val


def parse_fwhm(in_file):

    with open(in_file, "r") as infile:
        lines = infile.read()

    vals = " ".join(lines.split("\n")[-2].split()).split(" ")

    fwhm_x, fwhm_y, fwhm_z, fwhm_combined = vals

    return fwhm_x, fwhm_y, fwhm_z, fwhm_combined


# Got this from MRIQC
def fd_jenkinson(in_file, rmax=80., out_file=None):
    """
    Compute the :abbr:`FD (framewise displacement)` [Jenkinson2002]_
    on a 4D dataset, after AFNI-``3dvolreg`` has been executed
    (generally a file named ``*.affmat12.1D``).
    :param str in_file: path to epi file
    :param float rmax: the default radius (as in FSL) of a sphere represents
      the brain in which the angular displacements are projected.
    :param str out_file: a path for the output file with the FD
    :return: the output file with the FD, and the average FD along
      the time series
    :rtype: tuple(str, float)
    .. note ::
      :code:`infile` should have one 3dvolreg affine matrix in one row -
      NOT the motion parameters
    .. note :: Acknowledgments
      We thank Steve Giavasis (@sgiavasis) and Krishna Somandepali for their
      original implementation of this code in the [QAP]_.
    """

    import math
    import os.path as op
    import numpy as np

    if out_file is None:
        fname, ext = op.splitext(op.basename(in_file))
        out_file = op.abspath('{}_fdfile{}'.format(fname, ext))

    pm_ = np.genfromtxt(in_file)
    original_shape = pm_.shape
    pm = np.zeros((pm_.shape[0], pm_.shape[1] + 4))
    pm[:, :original_shape[1]] = pm_
    pm[:, original_shape[1]:] = [0.0, 0.0, 0.0, 1.0]

    # rigid body transformation matrix
    T_rb_prev = np.matrix(np.eye(4))

    flag = 0
    X = [0]  # First timepoint
    for i in range(0, pm.shape[0]):
        # making use of the fact that the order of aff12 matrix is "row-by-row"
        T_rb = np.matrix(pm[i].reshape(4, 4))

        if flag == 0:
            flag = 1
        else:
            M = np.dot(T_rb, T_rb_prev.I) - np.eye(4)
            A = M[0:3, 0:3]
            b = M[0:3, 3]

            FD_J = math.sqrt(
                (rmax * rmax / 5) * np.trace(np.dot(A.T, A)) + np.dot(b.T, b))
            X.append(FD_J)

        T_rb_prev = T_rb
    np.savetxt(out_file, X)
    return out_file


def extract_fd_results(in_file, cutoff=0.2):

    with open(in_file, "r") as infile:
        results = infile.read()

    # Parse the result into an array of floats
    res = list(map(float, results.strip().split("\n")))

    # Compute the mean framewise displacement
    mean_fd = sum(res) / len(res)

    # Get the FD values above the specified cutoff
    vals_above_cutoff = len(list(filter((lambda x: x > cutoff), res)))

    # Get the % of FD values above the specified cutoff
    perc_above_cutoff = float(vals_above_cutoff) / len(res) * 100

    return mean_fd, vals_above_cutoff, perc_above_cutoff
