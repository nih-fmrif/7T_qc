# Scripts for the analysis of 7T scanner data

## Converting Oxygen DICOM files into a BIDS-compatible directory structure
**WARNING: Remember DICOM files from Oxygen have PII - DO NOT load this data to Helix/Felix/Biowulf.**

### Step 1 - Create a virtual environment (named 'bids' in this example) on a machine with access to the raw data from Oxygen (assumes a working version of Anaconda is installed - does not matter if it's Anaconda2 or Anaconda3)

```
conda create --name bids python=3 -y
source activate bids
# Installs the scripts into the ~/scripts directory - Substitute this as appropriate
git clone https://github.com/nih-fmrif/7T_qc.git ~/scripts
cd ~/scripts
pip install -r requirements.txt
```

### Step 2 - Convert Oxygen DICOM data to BIDS

**The scans must be in a single top-level directory. They can be either in uncompressed or compressed (.tgz) form. Eg.**

* dicom_data/
  -- John_Doe_12345/
    * 2016-01-01-12345/
      -- mr_0001/
      -- mr_0002/
      -- ...
    * 2016-01-02-12345
      -- mr_0001/
      -- mr_0002/
      -- ...
    * ...

OR:

* dicom_data/
  -- John-Doe-12345_2016-01-01-12345.tgz
  -- Jane-Doe-12345_2016-01-01-12345.tgz
  -- ...

### Step 3 - Run
```
python ~/scripts/gen_bids.py /Users/myuser/bids_data/ /Users/myuser/oxygen_data/  \
--mapping_dir /Users/myuser/mappings/ --filters /Users/myuser/filters.json \
--log_dir /Users/myuser/logs/ --scanner_meta
```

Where:
 * **/Users/myuser/bids_data/** is the desired output directory for the BIDS data
 * **/Users/myuser/oxygen_data/** is the top-level directory containing the Oxygen DICOM files as specified above
 * **/Users/myuser/mappings/** is the directory where a JSON file containing a map from BIDS format to the Oxygen files (This file **WILL CONTAIN PII**, treat accordingly!)
 * **/Users/myuser/filters.json** is  JSON file with parameter to filter scans against, see below.
 * **/Users/myuser/logs/** is the directory where the logs should be saved.
 * **--scanner_meta** This flag appends scanner metadata to the mapping file.
 
 **To see all the available options run: `python ~/scripts/gen_bids.py -h`**
 
### A Note on Filters file
 
 * So far you can only filter by scanner sequence, i.e. create a JSON file with the allowable sequences. E.g.
 ```
 {
  "sequences": [
    "B.Poser@MBIC,NL",
    "epfid2d1_140",
    "epse2d1_144",
    "epfid2d1_142",
    "epfid2d1_160",
    "epfid2d1_126",
    "epfid2d1_130",
    "epse2d1_160",
    "epfid2d1_96",
    "epfid2d1_174",
    "epfid2d1_104",
    "epse2d1_104"
  ]
}
```


## Run Analysis Script on HPC

**If this program is ran in multiple threads, it will consume considerable memory. I recommend this to be run in the HPC.**

### Step 1 - Move data to HPC
`scp -r /Users/myuser/bids_data <username>@biowulf.nih.gov:/data/<dir>/bids_data`

### Step 2 - Create a virtual environment in the HPC (named 'tsnr' in this case)
```
module load python
conda create --name tsnr python=3 -y
source activate tsnr
# Installs the scripts into the ~/scripts directory - Substitute this as appropriate
git clone https://github.com/nih-fmrif/7T_qc.git ~/scripts
cd ~/scripts
pip install -r requirements.txt
```

### Step 3 - Create a bash script to run the analysis script (say, tsnr.sh):
```
#!/bin/bash

module load python
module load afni
source activate tsnr

python ~/scripts/run_analysis.py \
/data/<dir>/bids_data/ \
/data/<dir>/tsnr_analysis/ \
--log_dir /data/<dir>/logs/
```

Where:
* **/Users/myuser/bids_data/** is the input directory with the BIDS data
* **/Users/myuser/tsnr_analysis/** is the top-level output directory
* **--log_dir /Users/myuser/logs/** is the directory where the logs should be saved.

**To see all the available options run: `python ~/scripts/gen_bids.py -h`**

4. Submit batch job, eg.
`sbatch --partition=nimh --ntasks=1 --cpus-per-task=32 --mem=120g --time=10:00:00 tsnr.sh`

### Notes on performance:
* For the data currently stored in erbium, ~350 images where converted from DICOM to Nifti and
orgnanized into a BIDS directory structure. This took about 1 hour on magnesium with the settings as
specified above.
* For the resulting BIDS data (~350 .nii.gz images), running the analysis workflow on Biowulf took about
2 hours with the settings as specified above. At peak CPU usage, the script spawned about 720 threads. At peak
RAM usage, the script was consuming about 98 GB of memory. Allocate resources accordingly. 
