## Share ABCD minimally processed data


### Minimally-Processed-Image Sharing

Data and results from the ABCD Study are shared with the scientific community through The National Institute of Mental Health Data Archive (NDA).  The software described here uploads minimally-processed MRI (mproc) data to the NDA's database and Amazon Web Services (AWS-s3) buckets.  This software:
- Locates series information and minimally-processed data in a local file system,
- Finds, in NDA, a previously-shared fast-track record corresponding to the mproc series being shared
- Set a link between the new mproc data set and the previous fast-track NDA record (if more than one fast-track record found, we pick the last record in the NDA database)
- Loads an mproc record to NDA; this record summarizes information about participant, series type, and MRI parameters
- Assemblies an mproc BIDS data set, in the local file system, according to standards defined in: http://bids.neuroimaging.io/bids_spec.pdf
- Uploads the data set to the target AWS-s3 bucket; for Year-1 patch release: s3://abcd-mproc-patch/
- Records summary into a local SQL data base
- Logs the operation, summarizing the data source, data uploading success of failure, and error messages if any.

The software is composed of three scripts, one NDA-upload credentials file, one BIDS specification file, and AWS software and access credentials:
run_mproc_share.sh
share_min_proc_fMRI_dMRI_BOLD_T1T2.py
series_process_info_get.py
login_credentials.json
dataset_description.json
Access to NDA's AWS-s3 data -uploading buckets. Software must run on a computer on which AWS was installed, and set up with credentials provided by NDA.


### Description of scripts
run_mproc_share.sh
Reads a list of participants to share, a site, and a series type.  Uses share_min_proc_fMRI_dMRI_BOLD_T1T2.py to share all series from that site and series type to NDA's database and AWS-s3 bucket.

share_min_proc_fMRI_dMRI_BOLD_T1T2.py
Uploads minimally-processed data to NIH's NDA and Amazon Web Services (AWS-s3).  Uses series_process_info_get.py to find series in the local file system.

series_process_info_get.py
Locates processed image-series directories, files, processing information, and associated fast-track data in the local file system, for a given participant and MRI/fMRI modality.


### Uploading minimally-processed data to NDA
Execute
```
  ./run_mproc_share.sh  SubjsFile  Site  Modality
```

where:
  SubjsFile   List of participants to share (.csv); contains at least three columns: pGUIDs, anonymized date of birth, gender
  Site            An ABCD site: chla, daic, ..., yale
  Modality   T1, T2, dMRI, fMRI_MID_task, fMRI_SST_task, fMRI_nBack_task, rsfMRI

Example:
```
  ./run_mproc_share.sh  Subjs_Year1_patch_DTI.csv   chla  dMRI
  ./run_mproc_share.sh  Subjs_Year1_patch_BOLD.csv  chla  fMRI_MID_task
  ./run_mproc_share.sh  Subjs_Year1_patch_T1T2.csv  chla  T1
```

Written by Octavio Ruiz, based on code by Hauke Bartsch.
Last actualization: 2018aug23, for the ABCD Release 1.1 (Year-1 patch release).


### Checking BIDS compliance of a data set
In an empty, temporary directory (say "temp"), extract the contents of the compressed file.tgz:
tar  -xzvf  /mproc/site/filename.tgz
Delete the compressed file and check "temp" for BIDS compliance:
rm file.tgz
cd ..
bids-validator  temp
When test is complete, remove "temp"
