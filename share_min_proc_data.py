#!/usr/bin/env python3
"""
Collect processed imaging data, and share it on NDA

This script and associated resources are in: https://github...

Written by Hauke Bartsch & Octavio Ruiz, 2017nov16-
"""

# import sys, os, time, atexit, stat, tempfile, copy, tarfile, datetime, io, getopt
# import dicom, json, re, logging, logging.handlers, threading, pycurl, csv
# import struct
# from signal import SIGTERM
# from dicom.filereader import InvalidDicomError
# from dicom.dataelem import DataElement
# from dicom.tag import Tag
# from dicom.filebase import DicomBytesIO
# from multiprocessing import Pool, TimeoutError
# from io import StringIO
# import sqlite3
# import pandas as pd
# import hashlib

import sys, getopt, os
import logging, logging.handlers
import subprocess

# --------------------------------------------------------------------------------------------------------------------------------------------------------

def show_program_description( nothing=0 ):
    print('Usage:')
    print('  ./share_min_proc_data.py  --miNDAR DB  --qc QC  --share FsTkfname  --reference FileName  --outdir OutDir')
    print('where')
    print('  DB         Name of local minNDA database spreadsheet')
    print('  QC         Name of quality-control spreadsheet, listing participants to share')
    print('  FsTkfname  Name of associated file in our local fast-track directory')
    print('  FileName   Name of file to process and share, without path; must match NDA, without path')
    print('  OutDir     Local directory where processed file will be stored before sharing')
    print()

    print('For example:')
    print('./share_min_proc_data.py  --miNDAR ./orac_miNDAR.csv  --qc ./abcd_qc_t1.csv',
          ' --share Something',
          ' --reference NDARINVZZL0VA2F_baselineYear1Arm1_ABCD-fMRI-FM_20170323185349.tgz',
          ' --outdir ./temp' )
    print()
    return 0

# --------------------------------------------------------------------------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":

    # lfn = ''.join([ os.path.dirname(os.path.abspath(__file__)), os.path.sep, '/share_min_proc_data.log' ])
    # # logging.basicConfig(filename=lfn,format='%(levelname)s:%(asctime)s: %(message)s',level=logging.DEBUG)
    # log = logging.getLogger('MyLogger')
    # log.setLevel(logging.DEBUG)
    # handler = logging.handlers.RotatingFileHandler( lfn, maxBytes=1e+7, backupCount=5 )
    # handler.setFormatter(logging.Formatter('%(levelname)s:%(asctime)s: %(message)s'))
    # log.addHandler(handler)

    db_name = ''
    qc_name = ''
    FsTk_fname = ''
    fname = ''
    outdir = ''

    # --------------------------- Parse variables from command line ---------------------------
    if len(sys.argv) < 2:
        show_program_description()
        sys.exit()

    try:
        opts,args = getopt.getopt(sys.argv[1:],"hm:q:s:r:o:",["miNDAR=", "qc=", "share=", "reference=", "outdir="])
    except getopt.GetoptError:
        show_program_description()
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            show_program_description()
            sys.exit()
        elif opt in ("-m", "--miNDAR"):
            db_name = arg
        elif opt in ("-q", "--qc"):
            qc_name = arg
        elif opt in ("-s", "--share"):
            FsTk_fname = arg
        elif opt in ("-r", "--reference"):
            fname = arg
        elif opt in ("-o", "--outdir"):
            outdir = arg

    outdir = os.path.abspath(outdir)


    print('share.py: db_name =', db_name)
    print('share.py: qc_name =', qc_name)
    print('share.py: FsTk_fname =', FsTk_fname)
    print('share.py: fname =', fname)
    print('share.py: outdir =', outdir)

    # --------------------------- Check existence of files and directories ---------------------------
    # Get NDA series ID
    nda_id = ''
    rs = subprocess.run( ['./orac_NDAR_db.py', 'GetID', fname], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    rs_ok  = (rs.returncode == 0)
    rs_msg = rs.stdout.decode("utf-8")
    if not rs_ok:
        print('+ Unable to access local NDA data base +')
        sys.exit(0)
    # The NDAR databae has some repeated series, with different IDs' get the last one
    # For now I will take the first one
    nda_id = rs_msg.split('\n', 1)[-1].strip()
    print('nda_id =', nda_id )

    if not os.path.exists(outdir):
        print("Warning: output directory %s does not exist, try to create..." % outdir)
        os.mkdir(outdir)
        if not os.path.exists(outdir):
            print("Error: could not create output directory %s" % outdir)
            log.error("Error: could not create output directory %s" % outdir)
            sys.exit(0)


    # --------------------------- Generate output file ---------------------------

    # Set output file name
    ptrn        = 'ABCD-'
    minprc_type = 'ABCD-MPROC-'

    ss = fname.split( ptrn )
    if len(ss) != 2:
        print('Error: ----')
        exit()
    fname_out = ss[0] + minprc_type + ss[1]


    # Set output file-name extension
    fname_bas = ''
    fxpos = fname_out.rfind('.')
    if fxpos > 0:
        if fxpos > (len(fname_out) - 5):   # The rightmost '.' is near the end of filename, and thus consistent with extension
            fname_bas = fname_out[0:fxpos]
        else:
            fname_bas = fname_out
    else:
        if fxpos < 0:
            fname_bas = fname_out
        else:
            print('Error: invalid output file name')
            exit()
    if len(fname_bas) > 0:
        fname_out = fname_bas + '.nii'
    fname_out = outdir + '/' + fname_out


    print('Generating nifti file:', fname_out, '...')
    cmnd = '/usr/pubsw/packages/freesurfer/RH4-x86_64-R530/bin/mri_convert'

    # print('cmnd: ', ' '.join([cmnd, '-i', FsTk_fname, '-o', fname_out]) )

    rs = subprocess.run( [cmnd, '-i', FsTk_fname, '-o', fname_out], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    rs_ok  = (rs.returncode == 0)
    rs_msg = rs.stdout.decode("utf-8")
    if not rs_ok:
        print('Error: unable to convert file', FsTk_fname )
        sys.exit(0)

    print('done')
    exit()


# --------------------------------------------------------------------------------------------------------------------------------------------------------

    # print('QC spreadsheet contains NDA17 shared subjects only. Columns:')
    # print('id_redcap, redcap_event_name, site,qc_outcome, qc_fail_quest_reason')
