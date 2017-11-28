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

import sys, getopt, os, tarfile
import logging, logging.handlers
import subprocess

import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
pd.set_option('display.width', 512)

import csv


# ========================================================================================================================================================

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


def NDA_db_metadata( db_name, nda_id ):
    # Get subject's information from fast-track shared data NDA database
    record = {}

    dat = pd.read_csv( db_name )
    pd_record  =  dat[ dat['IMAGE03_ID'] == int(nda_id) ]
    pd_record.index = [0]

    record = {"SUBJECTKEY":      pd_record.get_value(0,'SUBJECTKEY'),
              "SRC_SUBJECT_ID":  pd_record.get_value(0,'SRC_SUBJECT_ID'),
              "DATASET_ID":      pd_record.get_value(0,'DATASET_ID'),
              "INTERVIEW_DATE":  pd_record.get_value(0,'INTERVIEW_DATE'),
              "INTERVIEW_AGE":   pd_record.get_value(0,'INTERVIEW_AGE'),
              "GENDER":          pd_record.get_value(0,'GENDER'),
              "IMAGE_FILE":      pd_record.get_value(0,'IMAGE_FILE'),
              "VISIT":           pd_record.get_value(0,'VISIT') }

    return record

# ========================================================================================================================================================




# ========================================================================================================================================================

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
    # ---------------------------------------------------------------------------------------------------------------------


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
    # ---------------------------------------------------------------------------------------------------------------------


    # ----------------------------------------------- Generate output file ------------------------------------------------
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
    fname_out_full = outdir + '/' + fname_out


    print('Generating nifti file in BIDS directory:', fname_out_full, '...')
    cmnd = '/usr/pubsw/packages/freesurfer/RH4-x86_64-R530/bin/mri_convert'
    rs = subprocess.run( [cmnd, '-i', FsTk_fname, '-o', fname_out_full], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    rs_ok  = (rs.returncode == 0)
    rs_msg = rs.stdout.decode("utf-8")
    if not rs_ok:
        print('Error: unable to convert file', fname_out_full )
        sys.exit(0)

    print('done')
    # ---------------------------------------------------------------------------------------------------------------------


    # ------------------------------------------------ Generate BIDS file -------------------------------------------------
    # BIDS format information:
    # https://images.nature.com/original/nature-assets/sdata/2016/sdata201644/extref/sdata201644-s1.pdf

    outtarname = ''.join([ os.path.abspath(outdir), os.path.sep, fname_bas, '.tgz' ])

    # Check if file already exists. If it does, exit; if not, continue

    nda = NDA_db_metadata( db_name, nda_id )

    subj  = nda['SUBJECTKEY'].replace('_','')
    visit = nda['VISIT'].replace('_','')
    bidstype = 'anat'
    scantype = 'T1w'

    # imageName = "sub-%s/ses-%s/%s/run-%s/%s.nii" % ( nda['SUBJECTKEY'].replace('_',''), nda['VISIT'], bidstype, run, fname_bas )
    imageName = "sub-%s/ses-%s/%s/sub-%s_ses-%s_%s.nii" % ( subj, visit, bidstype,
                                                            subj, visit, scantype )
    print('imageName:', imageName)

    msg = "Writing %s ..." % outtarname
    print(   msg)
#    log.info(msg)

    tarout = tarfile.open( outtarname, 'w:gz' )

    tarout.add( fname_out_full, arcname=imageName )

    tarout.add( 'dataset_description.json', arcname='dataset_description.json' )

    tarout.close()
    print('--- tarfile writen ---')


    # tinfo = tarfile.TarInfo( name=imageName )
    # # tarout.add( fname_out_full )
    # tarout.addfile( tinfo, fname_out_full )
    # ---------------------------------------------------------------------------------------------------------------------



    # ----------------------------- Record file upload metadata in local and NDA databases --------------------------------

    # Assembly NDA required information from our local spreadsheets, file system, and image files' metadata
    # We'll get additional info from fast-track NDA database, and NDA data dictionary

    # Calculate experiment ID fom image file metadata ----orsomethingelse?---- and NDA Data Dictionary
    exp_id = '-1234'   # This number will be provided by NDA, after we create new experiment types

    # Calculate experiment information from image file
    # Value must be one of these:
    # MR diffusion; fMRI; MR structural (MPRAGE); MR structural (T1); MR structural (PD); MR structural (FSPGR); MR structural (FISP); MR structural (T2); PET; ASL; microscopy; MR structural (PD, T2); MR structural (B0 map); MR structural (B1 map); single-shell DTI; multi-shell DTI; Field Map; X-Ray; static magnetic field B0
    # For T1:
    exp_scan = 'MR structural (T1)'

    # Assembly meta-data record to be savev to NDA and our local database
    # (according to specifications in fmriresults01_template.csv)
    record = {"subjectkey":        nda['SUBJECTKEY'],
              "src_subject_id":    nda['SRC_SUBJECT_ID'],
              "origin_dataset_id": nda['DATASET_ID'],
              "interview_date":    nda['INTERVIEW_DATE'],
              "interview_age":     nda['INTERVIEW_AGE'],
              "gender":            nda['GENDER'],
              "experiment_id": exp_id,
              "inputs":        'ABCD Fast-Track image data release for baseline assessments',
              "img03_id":      nda_id,
              "file_source":   nda['IMAGE_FILE'],
              "job_name":      '',
              "proc_types":    '',
              "metric_files":  '',
              "pipeline":         'MMPS version 245',
              "pipeline_script":  'MMIL_Preproc',
              "pipeline_tools":   'MMPS',
              "pipeline_type":    'MMPS',
              "pipeline_version": '245',
              "qc_fail_quest_reason": '',
              "qc_outcome":           'pass',
              "derived_files": 's3://...',   # a single s3 path to aTGZ file that contains the uploaded data in BIDS format
              "scan_type":     exp_scan,
              "img03_id2":     '',
              "file_source2":  '',
              "session_det":   'gradient unwarp, B1 inhomogeneity correction, resampled to 1mm^3 isotropic in LIA rigid body registration to non-MNI atlas',
              "image_history": '' }

    with open('newMinPrcsDat_NDA_record.csv', 'w') as f:
        w = csv.DictWriter(f, record.keys())
        w.writeheader()
        w.writerow(record)
    # ---------------------------------------------------------------------------------------------------------------------

# ========================================================================================================================================================





# --------------------------------------------------------------------------------------------------------------------------------------------------------

    # print('QC spreadsheet contains NDA17 shared subjects only. Columns:')
    # print('id_redcap, redcap_event_name, site,qc_outcome, qc_fail_quest_reason')

    # # Create BIDS container with processed images and metadata
    # bids_dir = 'bids_dir'
    # mkdir(            bids_dir )
    # mkdir( path.join( bids_dir, 'event' ) )
    # mkdir( path.join( bids_dir, 'event', 'anat') )

    # # Store image file in BIDS container
    # bids_dir_file = path.join( bids_dir, 'event', 'anat', fname_out )







#     # ------------------------------------------------ Generate BIDS file -------------------------------------------------

#     outtarname = ''.join([ os.path.abspath(outdir), os.path.sep, fname_bas, '.tgz' ])

#     # Check if file already exists. If it does, exit; if not, continue
    
#     msg = "Writing %s ..." % outtarname
#     print(   msg)
# #    log.info(msg)

#     tarout = tarfile.open( outtarname, 'w:gz' )

#     nda = NDA_db_metadata( db_name, nda_id )
#     bidstype = 'anat'   # Is this correct?
#     run = 1              # Where do I get this number?

#     # imageName = "sub-%s/ses-%s/%s/run-%s/%s" % ( nda['SUBJECTKEY'], nda['VISIT'], bidstype, run, fname_bas )
#     imageName = "sub-%s/ses-%s/%s/run-%s/%s.nii" % ( nda['SUBJECTKEY'].replace('_',''), nda['VISIT'], bidstype, run, fname_bas )

#     tarout.add( fname_out_full, arcname=imageName )

#     tarout.add( 'dataset_description.json', arcname='dataset_description.json' )

#     tarout.close()
#     print('--- tarfile writen ---')


#     # tinfo = tarfile.TarInfo( name=imageName )
#     # # tarout.add( fname_out_full )
#     # tarout.addfile( tinfo, fname_out_full )
#     # ---------------------------------------------------------------------------------------------------------------------
