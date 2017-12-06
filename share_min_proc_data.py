#!/usr/bin/env python3
"""
Collect processed imaging data, and share it on NDA

This script and associated resources are in: https://github...

Written by Hauke Bartsch & Octavio Ruiz, 2017nov16-dec05
"""

import sys, getopt, os, tarfile, datetime
import logging, logging.handlers
import subprocess, json
import sqlite3

import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
pd.set_option('display.width', 512)

import requests
import ast

import csv


# ========================================================================================================================================================

# ---------------------------------------------------------------------------------------------------------------------------------
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
# ---------------------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------------------
def command_line_get_variables():
    db_name = ''
    qc_name = ''
    FsTk_fname = ''
    fname = ''
    outdir = ''

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

    return db_name, qc_name, FsTk_fname, fname, outdir
# ---------------------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------------------
def NDA_db_metadata( db_name, nda_id ):
    # Get subject's information from fast-track shared data NDA database
    record = {}
    dat = pd.read_csv( db_name )
    # pd_record  =  dat[ dat['IMAGE03_ID'] == int(nda_id) ]
    pd_record  =  dat[ dat['IMAGE03_ID'] == nda_id ]
    pd_record.index = [0]
    record = {"SUBJECTKEY":      str( pd_record.get_value(0,'SUBJECTKEY')),
              "SRC_SUBJECT_ID":  str( pd_record.get_value(0,'SRC_SUBJECT_ID')),
              "DATASET_ID":      str( pd_record.get_value(0,'DATASET_ID')),
              "INTERVIEW_DATE":  str( pd_record.get_value(0,'INTERVIEW_DATE')),
              "INTERVIEW_AGE":   str( pd_record.get_value(0,'INTERVIEW_AGE')),
              "GENDER":          str( pd_record.get_value(0,'GENDER')),
              "IMAGE_FILE":      str( pd_record.get_value(0,'IMAGE_FILE')),
              "VISIT":           str( pd_record.get_value(0,'VISIT')) }
    return record
# ---------------------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------------------
def NIfTI_file_create( fname, type0, type_new ):
    fname_bas = ''
    fname_image = ''

    # Set output file name
    ss = fname.split( type0 )
    if len(ss) != 2:
        print('Error: file name is too short')
        sys.exit(0)

    fname_image = ss[0] + type_new + ss[1]

    # Set output file-name extension
    fxpos = fname_image.rfind('.')
    if fxpos > 0:
        if fxpos > (len(fname_image) - 5):   # The rightmost '.' is near the end of filename, and thus consistent with extension
            fname_bas = fname_image[0:fxpos]
        else:
            fname_bas = fname_image
    else:
        if fxpos < 0:
            fname_bas = fname_image
        else:
            print('Error: invalid output file name')
            sys.exit(0)

    if len(fname_bas) > 0:
        fname_image = fname_bas + '.nii'

    print('Generating NIfTI file:', fname_image, '...')

    # Convert image file into a NIfTI file, using dcm2nii
    cmnd = '/usr/pubsw/packages/freesurfer/RH4-x86_64-R530/bin/mri_convert'

    rs = subprocess.run( [cmnd, '-i', FsTk_fname, '-o', fname_image], stdout=subprocess.PIPE, stderr=subprocess.PIPE )

    rs_ok  = (rs.returncode == 0)
    rs_msg = rs.stdout.decode("utf-8")
    if not rs_ok:
        print('Error: unable to convert file', FsTk_fname, 'to NIfTI', fname_image)
        sys.exit(0)

    return fname_bas, fname_image
# ---------------------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------------------
def BIDS_file_create( outdir, fname_bas, fname_image, nda ):
    outtarname = ''
    res_ok = False

    # Write a tar file containing a BIDS-complying directory structure
    outtarname = ''.join([ outdir, os.path.sep, fname_bas, '.tgz' ])

    if os.path.exists(outtarname):
        msg = "BIDS file already exists: %s" % outtarname
        print( 'Error:', msg )
        log.error( msg + ", stop processing." )
        outtarname = ''
        return False, ''

    # BIDS format specification is described in
    # https://images.nature.com/original/nature-assets/sdata/2016/sdata201644/extref/sdata201644-s1.pdf

    subj  = nda['SUBJECTKEY'].replace('_','')
    visit = nda['VISIT'].replace('_','')
    bidstype = 'anat'
    scantype = 'T1w'

    imageName = "sub-%s/ses-%s/%s/sub-%s_ses-%s_%s.nii" % ( subj, visit, bidstype,
                                                            subj, visit, scantype )

    msg = "Adding  %s  to  %s ..." % (imageName, outtarname)
    print( msg )
    log.info( msg )

    tarout = tarfile.open( outtarname, 'w:gz' )
    tarout.add( fname_image, arcname=imageName )
    tarout.add( 'dataset_description.json', arcname='dataset_description.json' )
    tarout.close()

    return True, outtarname
# ---------------------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------------------
def miNDA_record_upload( metadata ):
    miNDA_ok = False
    miNDA_msg = ''

    try:
        with open('login_credentials.json','r') as f:
            try:
                login_credentials = json.load(f)
            except ValueError:
                print("Error: could not read login_credentials.json in the current directory or syntax error")
                log.error("Error: could not read login_credentials.json in the current directory or syntax error")
                sys.exit(0)

    except IOError:
        print("Error: unable to read login_credentials.json file in the current directory")
        log.error("Error: could not read login_credentials.json in the current directory")
        sys.exit(0)

    username = login_credentials['miNDAR']['username']
    password = login_credentials['miNDAR']['password']

    # Assembly package from metadata
    package = {
        "schemaName": "abcd_upload_107927",
        "dataStructureRows": [ {
                "shortName":  'fmriresults01',
                "dataElement": []
        } ]
    }
    for i,v in metadata.items():
        t = v
        if isinstance(t, list):
            t = json.dumps(t)
        package['dataStructureRows'][0]['dataElement'].append( { "name": i, "value": t } )

    # print('\npackage to upload to miNDA:')
    # print( json.dumps(package) )

    # Upload metadata package
    res = requests.post( "https://ndar.nih.gov/api/mindar/import",
                        auth=requests.auth.HTTPBasicAuth(username, password),
                        headers={'content-type':'application/json'},
                        data = json.dumps(package) )
    miNDA_ok  = res.ok
    miNDA_msg = res.text

    return miNDA_ok, miNDA_msg
# ---------------------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------------------
def AWS_file_upload( filename, AWS_bucket ):
    s3_ok  = False
    s3_msg = ''

    # AWS must be configured, in the computer running this process, with the appropriate credentials

    rs  =  subprocess.run( ['/home/oruiz/.local/bin/aws', 's3', 'cp', filename, AWS_bucket], stderr=subprocess.PIPE )
    s3_ok  = (rs.returncode == 0)
    s3_msg = rs.stderr.decode("utf-8")

    return s3_ok, s3_msg
# ---------------------------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------------------------
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def createMetaDataDB( metadatadir, table_name, metadata ):
    sqlite_file = ''.join([metadatadir, '/', 'metadata.sqlite'])    # name of the sqlite database file

    # Connecting to the database file
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    # Creating a new SQLite table
    c.execute('CREATE TABLE {tn} (id INTEGER PRIMARY KEY)'.format(tn=table_name))
    for key in metadata:
        c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}".format(tn=table_name,cn=key,ct="TEXT"))

    # Committing changes and closing the connection to the database file
    conn.commit()
    conn.close()
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

def addMetaData( metadatadir, table_name, metadata ):
    """       
      Store NDA-type information into local sqlite3 database
    """
    sqlite_file = ''.join([metadatadir, '/', 'metadata.sqlite'])    # name of the sqlite database file

    # if db does not exist already, create it
    if not os.path.isfile(sqlite_file):
            createMetaDataDB(metadatadir, table_name, metadata)
            if not os.path.isfile(sqlite_file):
                    print("Error: Could not create a database file at %s" % sqlite_file)
                    return

    # Connecting to the database file
    conn = 0
    try:
        conn = sqlite3.connect(sqlite_file)
    except sqlite3.Error:
        print("Warning: Could not connect to database file %s... wait and try again." % sqlite_file)
        time.sleep(1)
        try:
                conn = sqlite3.connect(sqlite_file)
        except sqlite3.Error:
                print("Error: Could not connect to database file %s" % sqlite_file)
                return
        pass

    c = conn.cursor()

    # Assembly record
    keys = []
    values = []
    for key in metadata:
        keys.append(key)
        if not isinstance(metadata[key], str):
            values.append(''.join(['"', str(metadata[key]), '"']))
        else:
            values.append(''.join(['"', metadata[key], '"']))

    c.execute("INSERT INTO {tn} ({cn}) VALUES ({val})".format(tn=table_name, cn=(','.join(keys)), val=(','.join(values))))

    conn.commit()
    conn.close()
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ---------------------------------------------------------------------------------------------------------------------------------

# ========================================================================================================================================================




# ========================================================================================================================================================

if __name__ == "__main__":

    # ---------------------------------------------- Initialize log -------------------------------------------------
    lfn = ''.join([ os.path.dirname(os.path.abspath(__file__)), os.path.sep, '/share_min_proc_data.log' ])
    log = logging.getLogger('MyLogger')
    log.setLevel(logging.DEBUG)
    handler = logging.handlers.RotatingFileHandler( lfn, maxBytes=1e+7, backupCount=5 )
    handler.setFormatter(logging.Formatter('%(levelname)s:%(asctime)s: %(message)s'))
    log.addHandler(handler)
    # ---------------------------------------------------------------------------------------------------------------


    # --------------------------------------- Get variables from command line ---------------------------------------
    db_name, qc_name, FsTk_fname, fname, outdir  =  command_line_get_variables()
    metadatadir = outdir

    print('\nshare_min_proc_data.py:')
    # print('db_name :   ', db_name)
    # print('qc_name :   ', qc_name)
    # print('FsTk_fname: ', FsTk_fname)
    # print('fname :     ', fname)
    # print('outdir :    ', outdir)
    # print('metadatadir:', metadatadir)
    # ---------------------------------------------------------------------------------------------------------------


    # ---------------------------------- Check existence of files and directories -----------------------------------

    if not os.path.exists(outdir):
        print("Warning: output directory %s does not exist, try to create..." % outdir)
        os.mkdir(outdir)
        if not os.path.exists(outdir):
            print("Error: could not create output directory %s" % outdir)
            log.error("Error: could not create output directory %s" % outdir)
            sys.exit(0)
    # ---------------------------------------------------------------------------------------------------------------


    # -------------------- We need to link previously fast-track upload data with this upload  ----------------------
    # -------------------- Get NDA key to link both data uploads ----------------------------------------------------
    # Get NDA series ID
    nda_id = ''
    rs = subprocess.run( ['./orac_NDAR_db.py', 'GetID', fname], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
    rs_ok  = (rs.returncode == 0)
    rs_msg = rs.stdout.decode("utf-8")
    if not rs_ok:
        print('+ Unable to access local NDA data base +')
        sys.exit(0)

    # The NDAR database has some repeated series with different IDs; orac_NDAR_db returns the IDs as a list of integers

    # print( 'rs_msg = ', rs_msg )
    # print( 'type(rs_msg):', type(rs_msg) )
    # print( 'len(rs_msg) =', len(rs_msg) )

    nda_id_list = ast.literal_eval( rs_msg )
    # print('nda_id_list:', nda_id_list )

    # Keep last nda_id, that presumably corresponds to last upload
    nda_id = nda_id_list[-1]
    print('nda_id =', nda_id )

    nda = NDA_db_metadata( db_name, nda_id )
    # ---------------------------------------------------------------------------------------------------------------


    # ---------------------------------- Create temporary NIfTI image file ------------------------------------------
    type0 = 'ABCD-'
    minprc_type = 'ABCD-MPROC-'
    
    fname_bas, fname_image  =  NIfTI_file_create( fname, type0, minprc_type )
    # ---------------------------------------------------------------------------------------------------------------


    # ---------------------------------------- Assembly BIDS object -------------------------------------------------

    # Incorporate NIfTI file to BIDS object
    res_ok, outtarname  =  BIDS_file_create( outdir, fname_bas, fname_image, nda )

    # Remove temporary NIfTI file
    print('Removing NIfTI file:', fname_image, '...')
    try:
        os.remove( fname_image )
    except Exception as e:
        print('Error: unable to remove temporary file', fname_image, e)

    if not res_ok:
        # BIDS file already exists
        sys.exit(0)
    # ---------------------------------------------------------------------------------------------------------------


    # ----------------------------- Record file upload metadata in local and NDA databases --------------------------

    #      Assembly meta-data record to be saved to NDA and our local database
    #      Use information from our local spreadsheets, file system, and image files' metadata;
    #      additional info from fast-track NDA database, and NDA data dictionary

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 	# Calculate metadata for current image series and processing stage

    # NDA: Data Dictionary: Title:      Processed MRI Data
    # NDA: Data Dictionary: Short Name: fmriresults01
    local_db_table = 'fmriresults01'

    #                         Convert study date to NDA format
    # 
    # Date read from NDA is in format '2016-10-12 03:00:00'
    # Record to upload requires format "04/06/2017 00:00:00", so convert
    datetime_orig = nda['INTERVIEW_DATE']
    dt_obj = datetime.datetime.strptime( datetime_orig, '%Y-%m-%d %H:%M:%S' )
    datetime_reformated = dt_obj.strftime( '%m/%d/%Y %H:%M:%S' )


    # Experiment ID is empty for structural imaging. For fMRI, a number will be provided by the NDA dictionary after we create new experiment types
    exp_id = ''

    # Type of scan; value must be one of these:
    #   MR diffusion; fMRI; MR structural (MPRAGE); MR structural (T1); MR structural (PD); MR structural (FSPGR); MR structural (FISP); MR structural (T2);
    #   PET; ASL; microscopy; MR structural (PD, T2); MR structural (B0 map); MR structural (B1 map); single-shell DTI; multi-shell DTI; Field Map;
    #   X-Ray; static magnetic field B0
    # There is not a value exclusively for minimally processed structural T1, we will use
    exp_scan = 'MR structural (T1)'

    # Description of project, processing stage (none = fast-track,  minimally-processed,  processed, ...)
    # and summarized description of processing
    session_det   = 'ABCD-MPROC-T1'
    image_history = 'gradient unwarp, B1 inhomogeneity correction, resampled to 1mm^3 isotropic in LIA rigid body registration to non-MNI atlas'
    
    AWS_bucket = 's3://abcd-mproc/'
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Assembly NDA-complying meta-data record (according to specifications in fmriresults01_definitions-2.csv)
    record = {"subjectkey":        nda['SUBJECTKEY'],
              "src_subject_id":    nda['SRC_SUBJECT_ID'],
              "origin_dataset_id": nda['DATASET_ID'],
              "interview_date":    datetime_reformated,
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
              "derived_files": AWS_bucket + fname_bas + '.tgz',
              "scan_type":     exp_scan,
              "img03_id2":     '',
              "file_source2":  '',
              "session_det":   session_det,
              "image_history": image_history }
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # Report NDA record
    with open('newMinPrcsDat_NDA_record.csv', 'w') as f:
        w = csv.DictWriter(f, record.keys())
        w.writeheader()
        w.writerow(record)
	# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # ---------------------------------------------------------------------------------------------------------------


    # ------------------------------ Upload record to miNDA and BIDS strucutre to AWS -------------------------------

    miNDA_ok, miNDA_msg  =  miNDA_record_upload( record )

    print('\nmiNDA_ok =', miNDA_ok)
    print(  'miNDA_msg:', miNDA_msg, '\n')

    if miNDA_ok:
        s3_ok, s3_msg  =  AWS_file_upload( outtarname, AWS_bucket )
    else:
        s3_ok  = ''
        s3_msg = 'AWS-s3 not attempted because miNDA upload failed'

    print('s3_ok =', s3_ok)
    print('s3_msg:', s3_msg)
    # ---------------------------------------------------------------------------------------------------------------


    # ----------------------------------- Upload record to local SQLite database ------------------------------------
    local_record = record

    local_record['miNDA_ok']  = miNDA_ok
    local_record['miNDA_msg'] = miNDA_msg
    local_record['miNDA_msg'] = local_record['miNDA_msg'].replace('\"','')
    local_record['miNDA_msg'] = local_record['miNDA_msg'].replace('\'','')

    local_record['s3_ok']  = s3_ok
    local_record['s3_msg'] = s3_msg
    local_record['s3_msg'] = local_record['s3_msg'].replace('\"','')
    local_record['s3_msg'] = local_record['s3_msg'].replace('\'','')

    addMetaData( metadatadir, local_db_table, local_record )
    # ---------------------------------------------------------------------------------------------------------------

    msg = "share_min_proc_data: End"
    print( '\n', msg )
    log.info( msg )
    sys.exit(0)
# ========================================================================================================================================================
