#!/bin/bash

dir=`dirname $0`

# ------------------------------------------------------------------------------------------------------------------
if [ ! "$#" = "3" ]; then
    echo
    echo "Upload minimally-processed data results to AWS-s3, and records to miNDA"
    echo "by calling share_min_proc_fMRI_dMRI_BOLD_T1T2.py"
    echo ""
    echo "               Written by Hauke Bartsch & Octavio Ruiz, 2017nov16-dec05"
    echo "               Modified by Octavio Ruiz,  2017dec21-2018jan17, aug07-23"
    echo "Usage:"
    echo "  ./run_mproc_share.sh  SubjsFile  Site  Modality"
    echo ""
    echo "where:"
    echo "  SubjsFile   Table (.csv) listing pGUIDs, anonymized dob, gender"
    echo "  Site        ABCD site: chla, daic, ..., yale"
    echo "  Modality    One day it will be:  T1, T2, dMRI, fMRI_MID_task, fMRI_SST_task, fMRI_nBack_task, rsfMRI"
    echo ""
    echo "Example:"
    echo "  ./run_mproc_share.sh  Subjs_Year1_patch_DTI.csv   chla  dMRI"
    echo "  ./run_mproc_share.sh  Subjs_Year1_patch_BOLD.csv  chla  fMRI_MID_task"
    echo "  ./run_mproc_share.sh  Subjs_Year1_patch_T1T2.csv  chla  T1"
    echo ""
    exit 0
fi
fdemog="$1"
site="$2"
modality="$3"

# ------------------------------------------------------------------------------------------------------------------
# List of participants to share:
# Currently (2018aug01), this list is the intersection of REDCap year-1 subjects and the subjects-to-share .json file that Hauke and Laura gave me.
# The resulting table is in both of these files (4516 subjects):
#   ~/share_R01_mproc_patch> 
#       327281 Aug  1 13:53 Subjs_Year1_patch.csv
#       275477 Aug  1 13:53 Subjs_Year1_patch.json
# Subjs_Year1_patch.csv has columns:
#   pGUID  mrif_score  site  dir  event_rc  manuf  release  dob  gender

qc=$fdemog

# check if list of participants to share exists
if [ ! -e "${qc}" ]; then 
   echo "Error: could not find list of participants to share: ${qc}"
   exit -1
fi

# TEST:
# subset=`cat $qc | grep "$site" | tail`
# echo $subset
# echo ""
# :TEST
# ------------------------------------------------------------------------------------------------------------------


if [   "$site" = "ucsd" ];  then
    site2="daic"
elif [ "$site" = "umb" ];   then
    site2="oahu"
elif [ "$site" = "wustl" ]; then
    site2="washu"
else
    site2=$site
fi


# ------------------------------------------------------------------------------------------------------------------
# For each subject in list to share, find minimally-processed container with results for requested modality,
# corresponding raw-data in /fast-track, create BIDS-compliant file, share it to AWS-s3, record operation to NDA,
# and update our records and logs.
echo ""

IFS=$'\n'
for line in `cat $qc | grep $site`;  do
    # echo "line: $line"
    subject=`echo $line    | cut -d',' -f1`
    subject=`echo $subject | cut -d'_' -f2`
    echo "PROCESSING subject: $subject"

    # --------------------------------------------------------------------------------------------
    # Call script that
    # (1) finds processed-series file names, bvals and bvec file names, registration matrix, and associated fast-track file name
    # (2) uploads the processed data to AWS, and a record to miNDA that includes a link to previously uploaded fast-track series

    # # TEST:
    # cmd="${dir}/share_min_proc_fMRI_dMRI_BOLD.py  --subject $subject  --demog $fdemog  --modality $modality  --NDAdb /home/oruiz/ABCD_Inventory/NDA_downloaded_packages/image03.txt  --outdir /mproc/$site2  --nowrite"
    # echo $cmd
    # ${dir}/share_min_proc_fMRI_dMRI_BOLD.py  --subject $subject  --demog $fdemog  --modality $modality  --NDAdb /home/oruiz/ABCD_Inventory/NDA_downloaded_packages/image03.txt  --outdir /mproc/$site2  --nowrite
    # # :TEST

    cmd="${dir}/share_min_proc_fMRI_dMRI_BOLD_T1T2.py  --subject $subject  --demog $fdemog  --modality $modality  --NDAdb /home/oruiz/ABCD_Inventory/NDA_downloaded_packages/image03.txt  --outdir /mproc/$site2"
    echo $cmd
     ${dir}/share_min_proc_fMRI_dMRI_BOLD_T1T2.py  --subject $subject  --demog $fdemog  --modality $modality  --NDAdb /home/oruiz/ABCD_Inventory/NDA_downloaded_packages/image03.txt  --outdir /mproc/$site2
    # --------------------------------------------------------------------------------------------

    echo ""

done
# ------------------------------------------------------------------------------------------------------------------
