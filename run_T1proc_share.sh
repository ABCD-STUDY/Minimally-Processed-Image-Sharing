#!/bin/bash

# Usage:
#   ./run_T1proc_share.sh
#   ./run_T1proc_share.sh --generate_oracle_db
#   ./run_T1proc_share.sh YALE
# 
# Written by Hauke Bartsch & Octavio Ruiz, 2017nov16-dec05


dir=`dirname $0`
db="${dir}/orac_miNDAR.csv"

# I am not sure if we should request the local database creation from this script
if [ "$?" > "0" ] && [ "$1" = "--generate_oracle_db" ]; then
    # Call oracle to generate a .csv local oracle database, that will be accessed by share_min_proc_data.sh, below
    export ORACLE_HOME=/usr/lib/oracle/12.2/client64/bin/
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/oracle/12.2/client64/lib/
    export DYLD_LIBRARY_PATH=/usr/lib/oracle/12.2/client64/lib/

    ${dir}/orac_NDAR_db.py CreateDataBase ${db}
    if [ -e "${db}" ]; then
        echo "created database file in ${db}"
        exit 0
    fi
    echo "Error: could not create database file in ${db}"
    exit 0
fi

if [ ! "$#" = "1" ]; then
    echo "Usage: provide a site name"
    exit 0
fi
site="$1"

# check if miNDAR database exists
if [ ! -e "${db}" ]; then
   echo "Error: could not find miNDAR database: ${db}"
   exit -1
fi

# QC spreadsheet with the following columns (contains NDA17 shared subjects only)
# id_redcap,redcap_event_name,site,qc_outcome,qc_fail_quest_reason
qc=${dir}/abcd_qc_t1.csv

# check if the QC information exists
if [ ! -e "${qc}" ]; then 
   echo "Error: could not find QC information: ${qc}"
   exit -1
fi


# process all participants in qc
site_upper=`echo "$site" | awk '{print toupper($0)}'`
subset=`cat "${qc}" | grep ",$site_upper,"`
# subset=`cat "${qc}"`
while read -r line; do

    echo ""
    echo "PROCESSING: qc_line: $line"
    subject=`echo $line | cut -d',' -f1`

    echo "PROCESSING: subject: $subject"


    # share a single participant's T1 minimally preprocessed data
    d=/space/syn05/1/data/MMILDB/DAL_ABCD_TEST/proc/
    subject=`echo $subject | cut -d'_' -f2`

    echo "PROCESSING: subject: $subject"


    T1=`ls ${d}/*${subject}*/MPR_res.mgz 2> /dev/null | head -1`

    echo "PROCESSING: min.processed image file: $T1"

    if [ ! -e "${T1}" ]; then
        echo "Unable to find minimally-processed image file"
        continue
    fi


    echo "getting MPR_res.mgz worked! got: $T1"
    # Do we have raw data shared for this participant?
    # Get file name, remove path, and check if it is fast-track shared, and present in NDA
    fastrk_file_path=`ls /fast-track/*/*${subject}*ABCD-T1*.tgz 2> /dev/null | head -1`

    echo "PROCESSING: fastrk_file_path: $fastrk_file_path"

    if [ ! -e "$fastrk_file_path" ]; then
       echo "Unable to find fast-track file"
       continue
    fi


    echo "getting ABCD-T1 as : $fastrk_file_path"
    file_name=`echo ${fastrk_file_path} | rev | cut -d'/' -f1 | rev`

    echo "PROCESSING: filename: $file_name"


    cmd="${dir}/share_min_proc_data.py --miNDAR ${db} --qc ${qc} --share ${T1} --reference $file_name --outdir /mproc/$site"
    echo $cmd
    echo $cmd >> ${dir}/log.log
    exec $cmd 2>&1 | tee -a ${dir}/log.log

    echo ""

done <<< "$subset"