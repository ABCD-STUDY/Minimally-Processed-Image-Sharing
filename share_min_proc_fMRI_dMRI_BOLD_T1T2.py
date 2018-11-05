#!/usr/bin/env python3

import sys, getopt, os, tarfile, datetime, io
import logging, logging.handlers
import subprocess, json
import sqlite3

import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
# warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
pd.set_option('display.width', 1024)
pd.set_option('max_colwidth', 200)

import requests
import ast
import csv
from scipy.io import loadmat
import math

from series_process_info_get import Get_File_Names_and_Process_Info

# ---------------------------------------------------------------------------------------------------------------------------------
AWS_bucket  = 's3://abcd-mproc-patch/'

modality_list = ['T1',    'T2',   'dMRI', 'fMRI_MID_task', 'fMRI_SST_task', 'fMRI_nBack_task', 'rsfMRI']
scantype_list = ['MPR', 'XetaT2',  'DTI',     'BOLD',           'BOLD',           'BOLD',       'BOLD' ]
BIDSsufx_list = [ '',      '',      '',       'mid',            'sst',            'nback',      'rest' ]
NDAexpid_list = [ '',      '',      '',       '648',            '650',            '651',        '649'  ]

scantype_for_modality  =  dict( zip( modality_list, scantype_list) )
bidsufix_for_modality  =  dict( zip( modality_list, BIDSsufx_list) )
NDAexpid_for_modality  =  dict( zip( modality_list, NDAexpid_list) )

TEST_MODE = False    # Can be changed through command line
# ---------------------------------------------------------------------------------------------------------------------------------


# ========================================================================================================================================================
# ---------------------------------------------------------------------------------------------------------------------------------
def show_program_description():
    print()
    print("Upload minimally-processed data to NIH's NDA and Amazon Web Services (AWS-s3):")
    print('locates series information and minimally-processed data in our file system,')
    print('finds, in a downloaded NDA dabatase package, a matching previously-uploaded fast-track record,')
    print('sets a link between the new mproc data set and the previous fast-track NDA record (if more than one fast-track record found, pick last record in table),')
    print('loads record to NDA,')
    print('assemblies a BIDS data set according to specification: http://bids.neuroimaging.io/bids_spec.pdf')
    print('uploads data set to AWS-s3 bucket: %s,' % AWS_bucket )
    print('and records a summary to a local SQL data base.')
    print("                                  Based on Hauke Bartsch's anonymizer.sh,  2017nov16 - 2018jan18")
    print('                                  Written by Octavio Ruiz,  2017nov16-2018jan18, 2018jul27-aug23')
    print()
    print('Usage:')
    print('  ./share_min_proc_fMRI_dMRI_BOLD_T1T2.py  --subject Subject  --demog SubjsFile  --modality Modality  --NDAdb DB  --outdir OutDir  ')
    print('  ./share_min_proc_fMRI_dMRI_BOLD_T1T2.py  --subject Subject  --demog SubjsFile  --modality Modality  --NDAdb DB  --outdir OutDir  --nowrite')
    print()
    print('where:')
    print('  Subject     Subject ID (without "NDAR" or "NDAR_" prefix)' )
    print('  SubjsFile   Table (.csv) listing pGUIDs, anonymized dob, gender (required), and other information')
    print('  Modality    Scan type: one of', modality_list )
    print('  DB          Path to a local, previously downloaded, NDA fast-track database package')
    print('  OutDir      Local directory to store assemblied BIDS data sets before sharing them')
    print('  --nowrite   Test mode: go through the process without uploading data to AWS-s3 or NDA')
    print()
    print('Examples:')
    print('  ./share_min_proc_fMRI_dMRI_BOLD_T1T2.py  --subject INV028D3ELL  --demog ./Subjs_Year1_patch_DTI.csv  --modality dMRI  --NDAdb /home/oruiz/ABCD_Inventory/NDA_downloaded_packages/image03.txt  --outdir test  --nowrite')
    print('  ./share_min_proc_fMRI_dMRI_BOLD_T1T2.py  --subject INVAU7FW44R  --demog ./Subjs_Year1_patch_DTI.csv  --modality dMRI  --NDAdb /home/oruiz/ABCD_Inventory/NDA_downloaded_packages/image03.txt  --outdir test  --nowrite')
    print('  ./share_min_proc_fMRI_dMRI_BOLD_T1T2.py  --subject INV4B1YV01D  --demog ./Subjs_Year1_patch_DTI.csv  --modality dMRI  --NDAdb /home/oruiz/ABCD_Inventory/NDA_downloaded_packages/image03.txt  --outdir test  --nowrite')
    print()
    print('  ./share_min_proc_fMRI_dMRI_BOLD_T1T2.py  --subject INV02EBX0JJ  --demog ./Subjs_Year1_patch_DTI.csv  --modality fMRI_MID_task    --NDAdb /home/oruiz/ABCD_Inventory/NDA_downloaded_packages/image03.txt  --outdir test  --nowrite')
    print('  ./share_min_proc_fMRI_dMRI_BOLD_T1T2.py  --subject INV0G2N59GL  --demog ./Subjs_Year1_patch_DTI.csv  --modality fMRI_SST_task    --NDAdb /home/oruiz/ABCD_Inventory/NDA_downloaded_packages/image03.txt  --outdir test  --nowrite')
    print('  ./share_min_proc_fMRI_dMRI_BOLD_T1T2.py  --subject INVZZZP87KR  --demog ./Subjs_Year1_patch_DTI.csv  --modality fMRI_nBack_task  --NDAdb /home/oruiz/ABCD_Inventory/NDA_downloaded_packages/image03.txt  --outdir test  --nowrite')
    print('  ./share_min_proc_fMRI_dMRI_BOLD_T1T2.py  --subject INV028D3ELL  --demog ./Subjs_Year1_patch_DTI.csv  --modality rsfMRI           --NDAdb /home/oruiz/ABCD_Inventory/NDA_downloaded_packages/image03.txt  --outdir test  --nowrite')
    print('"DTI" should be "BOLD", above')
    print()
    print('Dependences:')
    print('  Access to AWS-s3 from this computer')
    print('  ./Get_File_Names_and_Process_Info.py   To locates and read ContainerInfo.mat files')
    print('  ./login_credentials.json               Required to upload data to the NDA database')
    print()
    print('To check results use:')
    print('  bids-validator                                 Output data sets')
    print('  mri_info                                       Output NIfTI files')
    print('  ~/.local/bin/aws s3 ls s3://abcd-mproc-patch   Data sets received by AWS-s3, belonging to the Year-1 patch') 
    print()
# ---------------------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------------------
def command_line_get_variables():
    Proc_fname = ''
    db_fname   = ''
    outdir     = ''
    subjs_file = ''
    modality   = ''
    test_mode  = False

    # print("number of arguments found: %d\n" % len(sys.argv))
    if len(sys.argv) < 11  or len(sys.argv) > 12:
        show_program_description()
        sys.exit()

    try:
        opts,args = getopt.getopt(sys.argv[1:],"hs:d:m:n:o:w",["subject=", "demog=", "modality=", "NDAdb=", "outdir=", "nowrite"])
    except getopt.GetoptError as err:
        print("Error parsing arguments: %s" % str(err))
        show_program_description()
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            show_program_description()
            sys.exit()
        elif opt in ("-s", "--subject"):
            subject_id = arg
        elif opt in ("-d", "--demog"):
            subjs_file = arg
        elif opt in ("-m", "--modality"):
            modality = arg
        elif opt in ("-n", "--NDAdb"):
            db_fname = arg
        elif opt in ("-o", "--outdir"):
            outdir = arg
        elif opt in ("-w", "--nowrite"):
            test_mode = True

    outdir = os.path.abspath(outdir)

    if modality not in modality_list:
        print('Error: Modality must be one of', modality_list )
        sys.exit()

    return  subject_id, subjs_file, modality, db_fname, outdir, test_mode
# ---------------------------------------------------------------------------------------------------------------------------------
# ========================================================================================================================================================




# ========================================================================================================================================================
#                                               Functions used with all modalities

# ---------------------------------------------------------------------------------------------------------------------------------
def Subjects_File_Get_Subject( subject_id, subjs_fname ):
    
    subjs = pd.read_csv( subjs_fname, low_memory=False )

    return subjs[ subjs['pGUID'] == subject_id ]
# ---------------------------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------------------------
def Demog_Subject_Info_Get( subject_id, demog_file ):

    if os.path.exists( demog_file ):
        try:
            subjs = pd.read_json( demog_file )
        except ValueError:
            print("Error: could not read demographics data from file")
            log.error("Error: could not read demographics data from file")
            sys.exit(0)

    return subjs[ subjs['pGUID'] == subject_id ][['pGUID', 'dob', 'gender']]
# ---------------------------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------------------------
def NDA_db_Metadata_Get( db_fname, subject_id, FsTk_fname ):
    rec = pd.DataFrame()
    ok = True
    msg = ''

    # Read select columns from fast-track data package downloaded from NDA
    Series = pd.read_csv( db_fname, header=0, sep='\t', skiprows=[1], low_memory=False,
                          usecols=["image03_id", "dataset_id",
                                   "subjectkey", "interview_date", "interview_age", "gender",
                                   "image_file",
                                   "image_description",   # modality
                                   "experiment_id",
                                   'visit'] )

    Series = Series[ Series['subjectkey'] == subject_id ]

    if not len(Series):
        ok = False
        msg = 'Subject not found in NDA database package: %s. ' % db_fname
        return rec, ok, msg


    rec = Series[ [ FsTk_fname == os.path.basename(s)  for s in Series['image_file'] ] ]

    if not len(rec):
        ok = False
        msg = 'FsTk file name not found in NDA database package. '
        return rec, ok, msg

    if len(rec) > 1:
        msg = 'More than one FsTk file name match found in NDA database package; taking last entry. '
        rec = rec.iloc[-1:]

    #                 Convert study date to NDA format
    # Record to upload requires format "04/06/2017 00:00:00", so convert
    datetime_orig = rec['interview_date'].iat[0]
    dt_obj = datetime.datetime.strptime( datetime_orig, '%m/%d/%Y' )
    datetime_reformated = dt_obj.strftime( '%m/%d/%Y %H:%M:%S' )
    # rec['interview_date'] = datetime_reformated
    # rec.loc[:,'interview_date'] = datetime_reformated
    rec['interview_date'].iat[0] = datetime_reformated

    return rec, ok, msg
# ---------------------------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------------------------
def NIfTI_file_create( procfname, fstkfname, type0, type_new, TR, TE, TI, FlipAngle ):

    # Fast-track file name is used here to construct an output compressed nifti file,
    # as well as file name parts elsewhere
    fname_bas = ''
    fname_image = ''

    # Set output file name
    ss = fstkfname.split( type0 )
    if len(ss) != 2:
        print('Error (share_min_proc): unable to construct NIfTI file name')
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
            print('Error (share_min_proc): invalid NIfTI file name')
            sys.exit(0)

    if len(fname_bas) > 0:
        fname_image = '/tmp/' + fname_bas + '.nii'

    print('Generating NIfTI file:', fname_image, ',  with updated TR (and trying to update also TE, TI, and FlipAngle).')

    # For Year-1 release we used:
    # cmnd = '/usr/pubsw/packages/freesurfer/RH4-x86_64-R530/bin/mri_convert'
    # cmnd_and_args = [cmnd, '-i', procfname, '-o', fname_image]
    # 
    # For Year-1 patch release we want to set TR, TE, TI, and FlipAngle in the NIfTI file generated here.
    # mri_convert gives the options of setting
    #   -tr TR        in msec
    #   -te TE        in msec
    #   -TI TI        in msec (note upper case flag)
    #   -flip_angle   in radians
    # however, when converting NIfTI --> NIfTI, it only changes TR; there is no place in the NIfTI standard format for the othr parameters.
    # I write them anyway, because there may be an extension one day,
    # and I will include all the parameteres in the .json file inside the BIDS data set

    cmnd = '/usr/pubsw/packages/freesurfer/RH4-x86_64-R600/bin/mri_convert'

    TRstr        = '%f' % TR
    TEstr        = '%f' % TE
    FlipAnglestr = '%f' % math.radians( FlipAngle )
    if TI:
        TIstr    = '%f' % TI
        cmnd_and_args = [cmnd,  '-i', procfname,  '-o', fname_image, '-tr', TRstr,  '-te', TEstr,
                         '-TI', TIstr,  '-flip_angle', FlipAnglestr ]
    else:
        cmnd_and_args = [cmnd,  '-i', procfname,  '-o', fname_image, '-tr', TRstr,  '-te', TEstr,
                         '-flip_angle', FlipAnglestr ]
    print('Executing:', ' '.join( cmnd_and_args) )

    rs = subprocess.run( cmnd_and_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE )

    rs_ok  = (rs.returncode == 0)
    rs_msg = rs.stdout.decode("utf-8")
    if not rs_ok:
        print('Error (share_min_proc): unable to convert NIfTI file', procfname, 'to .mgz', fname_image )
        sys.exit(0)

    print()

    return fname_bas, fname_image
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
                print("Error: could not read miNDA login_credentials.json in the current directory or syntax error")
                log.error("Error: could not read miNDA login_credentials.json in the current directory or syntax error")
                sys.exit(0)

    except IOError:
        print("share_min_proc_data.py: Error: unable to read login_credentials.json file in the current directory")
        log.error("share_min_proc_data.py: Error: could not read login_credentials.json in the current directory")
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


    if TEST_MODE:
        print('\npackage to upload to miNDA:')
        print( json.dumps( package, sort_keys=True, indent=2 ) )
        miNDA_ok  = True
        miNDA_msg = "Here I would upload record to miNDA"
    else:
        # Upload metadata package
        res = requests.post( "https://ndar.nih.gov/api/mindar/import",
                            auth=requests.auth.HTTPBasicAuth(username, password),
                            headers={'content-type':'application/json'},
                            data = json.dumps(package) )
        miNDA_ok  = res.ok
        miNDA_msg = res.text

        if not miNDA_ok:
            print('\npackage to upload to miNDA:')
            print( json.dumps( package, sort_keys=True, indent=2 ) )


    return miNDA_ok, miNDA_msg
# ---------------------------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------------------------
def AWS_file_upload( filename ):
    # AWS must be configured, in the computer running this process, with the appropriate credentials
    s3_ok  = False
    s3_msg = ''

    if TEST_MODE:
        print('Would execute', ' '.join(['/home/oruiz/.local/bin/aws', 's3', 'cp', filename, AWS_bucket]) )
        s3_ok  = True
        s3_msg = "Here I would upload data set to AWS-s3"
    else:
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
                    print("share_min_proc_data.py: Error: Could not create a database file at %s" % sqlite_file)
                    return

    # Connecting to the database file
    conn = 0
    try:
        conn = sqlite3.connect(sqlite_file)
    except sqlite3.Error:
        print("share_min_proc_data.py: Warning: Could not connect to database file %s... wait and try again." % sqlite_file)
        time.sleep(1)
        try:
                conn = sqlite3.connect(sqlite_file)
        except sqlite3.Error:
                print("share_min_proc_data.py: Error: Could not connect to database file %s" % sqlite_file)
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

    if TEST_MODE:
        print()
        print('Here I would send to local database:')
        print(     "INSERT INTO {tn} ({cn}) VALUES ({val})".format(tn=table_name, cn=(','.join(keys)), val=(','.join(values))) )
    else:
        c.execute( "INSERT INTO {tn} ({cn}) VALUES ({val})".format(tn=table_name, cn=(','.join(keys)), val=(','.join(values))) )
        conn.commit()

    conn.close()
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# ---------------------------------------------------------------------------------------------------------------------------------
# ========================================================================================================================================================



# ========================================================================================================================================================
#                                         Functions to handle specific modalities

# ---------------------------------------------------------------------------------------------------------------------------------
def motion_file_read( fname ):
    motion = pd.read_csv( fname,  delim_whitespace=True,  header=None,  index_col=0,
                          names=['t_indx', 'rot_z', 'rot_x', 'rot_y', 'trans_z', 'trans_x', 'trans_y', 'nothing1', 'nothing2'] )
    motion = motion.drop( ['nothing1', 'nothing2'], axis='columns' )
    return motion.to_csv( sep='\t' )
# ---------------------------------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------------------------------
def registration_matrix_read( regmtx_f ):
    RegMtx = []
    try:
        data = loadmat( regmtx_f, squeeze_me=True, struct_as_record=True )
        Info = data['RegInfo']
        # Read matrix as a Python ndarray
        RegMtx = Info['M_T1_to_T2'].item()
        RegMtx = RegMtx.tolist()
    except:
        print('Warning: unable to read registration matrix', regmtx_f )
    return RegMtx
# ---------------------------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------------------------
def BIDS_file_check_and_name_parts( outdir, fname_bas, pGUID, visit, scantype ):
    # Construct file name elements according to BIDS format standard
    outtarname  = ''
    subj        = ''
    bids_visit  = ''
    bids_type   = ''
    bids_sufix  = ''
    bids_sufix2 = ''
    ok = False
    msg = ''

    outtarname = ''.join([ outdir, os.path.sep, fname_bas, '.tgz' ])

    if os.path.exists(outtarname):
        msg = "Error: BIDS file already exists: %s" % outtarname
        print( msg )
        log.error( msg )
        outtarname = ''
        return outtarname, subj, bids_visit, bids_type, bids_sufix, bids_sufix2, ok, msg


    subj  = pGUID.replace('_','')

    # Reconstruct visit ID according to /fast-track format: no dashes
    vv = visit.split('_')
    vlist = []
    vlist.append( vv[0] )
    vlist.extend( [s.title() for s in vv[1:]] )
    bids_visit = ''.join(vlist)

    if scantype == 'MPR':
        bids_type  = 'anat'
        bids_sufix = '_T1w'
        ok = True

    elif scantype == 'XetaT2':
        bids_type  = 'anat'
        bids_sufix = '_T2w'
        ok = True

    elif scantype == 'BOLD':
        bids_type  = 'func'
        bids_sufix = '_bold'
        bids_sufix2 = bidsufix_for_modality[modality]
        ok = True

    elif scantype == 'DTI':
        bids_type  = 'dwi'
        bids_sufix = '_dwi'
        ok = True

    else:
        msg = "Error: BIDS_file_check_and_name_parts: wrong scantype: %s. " % scantype
        print( msg )

    return outtarname, subj, bids_visit, bids_type, bids_sufix, bids_sufix2, ok, msg
# ---------------------------------------------------------------------------------------------------------------------------------



# ---------------------------------------------------------------------------------------------------------------------------------
# def BIDS_file_create_T1T2( outdir, fname_bas, fname_image, nda, scantype, registration_matrix, bvals, bvecs, duplicate ):

def BIDS_file_create_T1T2( outdir, fname_bas, fname_image, pGUID, visit, scantype,
                           run, TR, TE, TI, FlipAngle ):

    # Assembly and write tar file containing a structural(T1-or-T2)-MRI BIDS-complying data set

    outtarname = ''
    msg = ''
    res_ok = False

    outtarname, subj, bids_visit, bids_type, bids_sufix, bids_sufix2, ok, msg  =  BIDS_file_check_and_name_parts( outdir, fname_bas, pGUID, visit, scantype )
    
    if not ok:
        outtarname = ''
        return False, outtarname


    # Add image file
    imageName = "sub-%s/ses-%s/%s/sub-%s_ses-%s_%s%s.nii" % ( subj, bids_visit, bids_type,
                                                              subj, bids_visit, run, bids_sufix )
    msg = "Adding  %s  to  %s" % (imageName, outtarname)
    print( msg )
    log.info( msg )

    tarout = tarfile.open( outtarname, 'w:gz' )
    tarout.add( fname_image, arcname=imageName )


    # Add description file, required by BIDS
    tarout.add( 'dataset_description.json', arcname='dataset_description.json' )


    # Add "meta information about the acquisition" and registration matrix to accompanying .json file
    jsonName = "sub-%s/ses-%s/%s/sub-%s_ses-%s_%s%s.json" % ( subj, bids_visit, bids_type,
                                                              subj, bids_visit, run, bids_sufix )
    jsonContent = { 
        "RepetitionTime": TR / 1000,
        "EchoTime":       TE / 1000,
        "FlipAngle":      FlipAngle
    }
    if scantype == 'MPR':
        jsonContent.update( {"InversionTime":  TI / 1000} )

    jsonContentStr = json.dumps( jsonContent )
    tinfo = tarfile.TarInfo( name=jsonName )
    tinfo.size = len( jsonContentStr )
    tarout.addfile( tinfo, io.BytesIO(jsonContentStr.encode('utf8')) )


    # Close data set package and return
    tarout.close()

    return True, outtarname
# ---------------------------------------------------------------------------------------------------------------------------------



# ---------------------------------------------------------------------------------------------------------------------------------
def BIDS_file_create_BOLD( outdir, fname_bas, fname_image, pGUID, visit, scantype, modality,
                           motion_file, regis_file, event_file, run, TR, TE, FlipAngle ):

    # Assembly and write tar file containing a functional-MRI BIDS-complying data set

    outtarname = ''
    msg = ''
    res_ok = False

    outtarname, subj, bids_visit, bids_type, bids_sufix, bids_sufix2, ok, msg  =  BIDS_file_check_and_name_parts( outdir, fname_bas, pGUID, visit, scantype )
    
    if not ok:
        outtarname = ''
        return False, outtarname


    # Add image file
    imageName = "sub-%s/ses-%s/%s/sub-%s_ses-%s_task-%s_%s%s.nii" % ( subj, bids_visit, bids_type,
                                                                      subj, bids_visit, bids_sufix2, run, bids_sufix )
    msg = "Adding  %s  to  %s" % (imageName, outtarname)
    print( msg )
    log.info( msg )

    tarout = tarfile.open( outtarname, 'w:gz' )
    tarout.add( fname_image, arcname=imageName )


    # Add description file (required by BIDS)
    tarout.add( 'dataset_description.json', arcname='dataset_description.json' )


    # Add events file, if present; rsfMRI will not have one
    if event_file:
        tsvfName  = "sub-%s/ses-%s/%s/sub-%s_ses-%s_task-%s_%s%s.tsv" % ( subj, bids_visit, bids_type,
                                                                        subj, bids_visit, bids_sufix2, run, '_events' )
        tarout.add( event_file, arcname=tsvfName )


    # Add motion-correction table
    tsvfName  = "sub-%s/ses-%s/%s/sub-%s_ses-%s_task-%s_%s%s.tsv" % ( subj, bids_visit, bids_type,
                                                                      subj, bids_visit, bids_sufix2, run, '_motion' )
    tsv_str = motion_file_read( motion_file )

    print("Adding  %s  to  %s" % (tsvfName, outtarname) )

    tinfo = tarfile.TarInfo( name=tsvfName )
    tinfo.size = len( tsv_str )
    tarout.addfile( tinfo, io.BytesIO(tsv_str.encode('utf8')) )


    # Add "meta information about the acquisition" as a json file containing:
    # RepetitionTime and TaskName (required by BIDS), and registration matrix
    jsonName = "sub-%s/ses-%s/%s/sub-%s_ses-%s_task-%s_%s%s.json" % ( subj, bids_visit, bids_type,
                                                                      subj, bids_visit, bids_sufix2, run, bids_sufix )
    registration_matrix = registration_matrix_read( regis_file )

    if TEST_MODE:
        print('registration_matrix =', registration_matrix )

    jsonContent = {
        "TaskName":       bids_sufix2,
        "registration_matrix_T1": registration_matrix,
        "RepetitionTime": TR / 1000,
        "EchoTime":       TE / 1000,
        "FlipAngle":      FlipAngle
    }

    print("Adding  %s  to  %s" % (jsonName, outtarname) )

    jsonContentStr = json.dumps( jsonContent )
    tinfo = tarfile.TarInfo( name=jsonName )
    tinfo.size = len( jsonContentStr )
    tarout.addfile( tinfo, io.BytesIO(jsonContentStr.encode('utf8')) )


    # Close data set package and return
    tarout.close()

    return True, outtarname
# ---------------------------------------------------------------------------------------------------------------------------------



# ---------------------------------------------------------------------------------------------------------------------------------
def BIDS_file_create_DTI( outdir, fname_bas, fname_image, pGUID, visit, scantype, registration_matrix, bvals, bvecs, run,
                          TR, TE, FlipAngle ):
    # Assembly and write a tar file containing a BIDS-complying directory structure
    # BIDS format specification is described in: http://bids.neuroimaging.io/bids_spec.pdf

    outtarname = ''
    msg = ''
    res_ok = False

    outtarname, subj, bids_visit, bids_type, bids_sufix, bids_sufix2, ok, msg  =  BIDS_file_check_and_name_parts( outdir, fname_bas, pGUID, visit, scantype )

    if not ok:
        outtarname = ''
        return False, outtarname


    # Add image file
    imageName = "sub-%s/ses-%s/%s/sub-%s_ses-%s_%s%s.nii" % ( subj, bids_visit, bids_type,
                                                              subj, bids_visit, run, bids_sufix )
    msg = "Adding  %s  to  %s" % (imageName, outtarname)
    print( msg )
    log.info( msg )

    tarout = tarfile.open( outtarname, 'w:gz' )
    tarout.add( fname_image, arcname=imageName )


    # Add description file, required by BIDS
    tarout.add( 'dataset_description.json', arcname='dataset_description.json' )


    # Add bvals file (2018jul30: do not transpose)
    bvalName = "sub-%s/ses-%s/%s/sub-%s_ses-%s_%s%s.bval" % ( subj, bids_visit, bids_type,
                                                              subj, bids_visit, run, bids_sufix )
    with open(bvals, 'r') as f:
        bvals_str = f.read()
    tinfo = tarfile.TarInfo(name=bvalName)
    tinfo.size = len(bvals_str)
    tarout.addfile(tinfo, io.BytesIO(bvals_str.encode('utf8')))


    # Add bvecs file (2018jul30: do not transpose)
    bvecName = "sub-%s/ses-%s/%s/sub-%s_ses-%s_%s%s.bvec" % ( subj, bids_visit, bids_type,
                                                              subj, bids_visit, run, bids_sufix )
    with open(bvecs, 'r') as f:
        bvecs_str = f.read()
    tinfo = tarfile.TarInfo(name=bvecName)
    tinfo.size = len(bvecs_str)
    tarout.addfile(tinfo, io.BytesIO(bvecs_str.encode('utf8')))


    # Add "meta information about the acquisition" and registration matrix to accompanying .json file
    jsonName = "sub-%s/ses-%s/%s/sub-%s_ses-%s_%s%s.json" % ( subj, bids_visit, bids_type,
                                                              subj, bids_visit, run, bids_sufix )
    jsonContent = { 
        "registration_matrix_T1": registration_matrix,
        "IntendedFor": "sub-%s_ses-%s_%s%s.nii" % (subj, bids_visit, run, bids_sufix),
        "RepetitionTime": TR / 1000,
        "EchoTime":       TE / 1000,
        "FlipAngle":      FlipAngle
    }
    jsonContentStr = json.dumps( jsonContent )
    tinfo = tarfile.TarInfo( name=jsonName )
    tinfo.size = len( jsonContentStr )
    tarout.addfile( tinfo, io.BytesIO(jsonContentStr.encode('utf8')) )


    # Close data set package and return
    tarout.close()
    return True, outtarname
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


    # -------------------------------------- Interpret command-line variables ---------------------------------------

    subject_id, subjs_file, modality, db_fname, outdir, test_mode  =  command_line_get_variables()

    TEST_MODE = test_mode
    pGUID     = 'NDAR_'+subject_id
    scantype  = scantype_for_modality[modality]
    metadatadir = outdir
    # ---------------------------------------------------------------------------------------------------------------


    # ------------------------------- Get demographics information for this subject ---------------------------------

    try:
        subj_info = Subjects_File_Get_Subject( pGUID, subjs_file )
        print('subj_info:')
        print( subj_info, '\n' )
    except Exception as err:
        msg = "Error: unable to find subject's information: %s. " % str(err)
        print( msg, '\n')
        sys.exit(0)
    # ---------------------------------------------------------------------------------------------------------------


    # ------------------------------------ Check existence of output directory --------------------------------------
    if not os.path.exists(outdir):
        print("Warning: output directory %s does not exist, try to create..." % outdir)
        os.mkdir(outdir)
        if not os.path.exists(outdir):
            print("share_min_proc_data.py: Error: could not create output directory %s" % outdir)
            log.error("share_min_proc_data.py: Error: could not create output directory %s" % outdir)
            print()
            sys.exit(0)
        print()
    # ---------------------------------------------------------------------------------------------------------------


    # ---------------------------------------------------------------------------------------------------------------
    #             Get file names and additional information corresponding to this subject and modality:
    #             minimally-processed series, fast-track file names, and parameters like
    #             TR, TE, TI, FlipAngle, event_file, or registration matrix, depending on modality.
    #             Assembly BIDS data sets, and upload records to miNDA and data sets to AWS-s3.

    try:
        Proc_files = Get_File_Names_and_Process_Info( subject_id, modality )
    except:
        print('Error: unable to get series information\n')
        sys.exit(0)


    print('db_fname:   ', db_fname)
    print('outdir :    ', outdir)
    print('metadatadir:', metadatadir,   ',   scantype:', scantype,  ',   Processing:', Proc_files.keys() )

    if TEST_MODE:
        print('TEST_MODE = ', TEST_MODE )
        # print( json.dumps( Proc_files, sort_keys=True, indent=2 ) )

    if len(Proc_files.keys()) <= 0:
        print('No series to process from this subject')
        sys.exit(0)


    for j in range(1, len(Proc_files.keys())+1 ):
        print()

        key = 'Run-%02.0f'%j

        if TEST_MODE:
            print('key =', key )
            # print( Proc_files[key] )

        if key in Proc_files.keys():
            bids_run = key.lower()
            print('bids_run =  ', bids_run)

            # ---------------------------------------------------------------------------------------------------------------
            Proc_fname = Proc_files[key]['MinProc_file']
            print('Proc_fname: ', Proc_fname)

            FsTk_fname = Proc_files[key]['FasTrk_file_nopath']
            if FsTk_fname:
                if FsTk_fname == Proc_files[key]['FasTrk_file_Guessed_Name']:
                    comment = '  (same as guessed file name)'
                else:
                    comment = '  ( Different from guessed file name; using guessed )'
                    print('FsTk_fname: ', FsTk_fname, comment )
                    FsTk_fname = Proc_files[key]['FasTrk_file_Guessed_Name']
                    comment = '  (guessed)'
            else:
                FsTk_fname = Proc_files[key]['FasTrk_file_Guessed_Name']
                comment = '  (guessed)'
            print('FsTk_fname: ', FsTk_fname, comment )

            motion_file         = ''
            regis_file          = ''
            event_file          = ''
            registration_matrix = ''
            bvals               = ''
            bvecs               = ''

            if scantype in ['MPR', 'XetaT2']:
                pass

            elif scantype == 'BOLD':
                motion_file    = Proc_files[key]['Motion_file']
                regis_file     = Proc_files[key]['Regis_file']
                if 'Event_file' in Proc_files[key].keys():
                    event_file = Proc_files[key]['Event_file']
                else:
                    event_file = ''
                print('motion_file:', motion_file)
                print('regis_file: ', regis_file)
                print('event_file: ', event_file)

                if modality != 'rsfMRI' and not event_file:
                    print()
                    msg = "Error: task series require an events file, and we were unable to find it. "
                    print( msg, '\n')
                    sys.exit(0)

            elif scantype == 'DTI':
                registration_matrix = Proc_files[key]['RegistrationMatrix']
                bvals               = Proc_files[key]['bval_file']
                bvecs               = Proc_files[key]['bvec_file']
                print('Reg.Matrix =', registration_matrix)
                print('bvals:      ', bvals)
                print('bvecs:      ', bvecs)

            else:
                msg = "Error: invalid scantype: %s. " % scantype
                print( msg, '\n')
                sys.exit(0)


            series_date = Proc_files[key]['series_date']    # Used if we need to calculate interview date and age
            # series_time = Proc_files[key]['series_time']    # Used if we need to calculate interview date and age

            TR = Proc_files[key]['TR']
            TE = Proc_files[key]['TE']
            if 'TI' in Proc_files[key].keys():
                TI = Proc_files[key]['TI']
            else:
                TI = None
            FlipAngle = Proc_files[key]['FlipAngle']
            print('TR, TE, FlipAngle: ', TR, TE, FlipAngle )

            if len(Proc_fname) <= 0:
                print('Error: no image series to process')
                sys.exit(0)
            # ---------------------------------------------------------------------------------------------------------------


            # --------------------- Link this mproc series with previously fast-track uploaded data, ------------------------
            #                       through NDA key: image03_id

            ser_info = subj_info.copy()

            nda_fstk_record, nda_ok, msg  =  NDA_db_Metadata_Get( db_fname, pGUID, FsTk_fname )

            if nda_ok:
                print( msg )
                print('nda_fstk_record:')
                print( nda_fstk_record )
                if len( nda_fstk_record ):
                    nda_id = nda_fstk_record['image03_id'].item()
                else:
                    nda_id = ''
                # Convert record (one-row Pandas DataFrame) to a dictionary, to simplify variable extraction below
                nda_fstk_record = nda_fstk_record.to_dict( 'records')[0]
                print()

            else:
                nda_fstk_record = {}
                nda_id = ''
                # print('Error:', msg )
                # print('  So we skip this series without creating any BIDS dataset nor uploading anything to NDA or AWS-s3')
                print()
                msg = 'Warning: ' + msg + 'Using metadata from local sources: REDCap, Incoming.csv, and fixes. '
                print( msg, '\n' )

            ser_info['nda_id'] = nda_id

            print('ser_info:')
            print( ser_info, '\n' )

            # Convert ser_info (one-row Pandas DataFrame) to a dictionary, to simplify variable extraction below
            ser_info = ser_info.to_dict('records')[0]
            # ---------------------------------------------------------------------------------------------------------------


            # ---------------------------------- Create temporary NIfTI image file ------------------------------------------
            type0 = 'ABCD-'
            minprc_type = 'ABCD-MPROC-'

            fname_bas, fname_image  =  NIfTI_file_create( Proc_fname, FsTk_fname, type0, minprc_type, TR, TE, TI, FlipAngle )

            # 2018jul30: mri_convert can set TR in the NIfTI file, but not TE, TI, or FlipAngle.
            # So I am including these variables in the .json file, below
            # ---------------------------------------------------------------------------------------------------------------


            # ---------------------------------------- Assembly BIDS object -------------------------------------------------
            visit = ser_info['event_rc']
            # visit = nda_fstk_record['visit']

            # Create a BIDS data set and incorporate the NIfTI file
            if scantype in ['MPR', 'XetaT2']:

                res_ok, outtarname  =  BIDS_file_create_T1T2( outdir, fname_bas, fname_image, pGUID, visit, scantype,
                                                              bids_run, TR, TE, TI, FlipAngle )
            elif scantype == 'BOLD':

                res_ok, outtarname  =  BIDS_file_create_BOLD( outdir, fname_bas, fname_image, pGUID, visit, scantype, modality,
                                                              motion_file, regis_file, event_file, bids_run, TR, TE, FlipAngle )
            elif scantype == 'DTI':

                res_ok, outtarname  =  BIDS_file_create_DTI( outdir, fname_bas, fname_image, pGUID, visit, scantype,
                                                             registration_matrix, bvals, bvecs, bids_run,
                                                             TR, TE, FlipAngle )
            else:
                print('Error: scantype', scantype, 'not implemented here')
                print()
                sys.exit(0)


            # Remove temporary NIfTI file
            print('Removing NIfTI file:', fname_image)
            try:
                os.remove( fname_image )
            except Exception as e:
                print('Error: unable to remove temporary file', fname_image, e)

            if not res_ok:
                print()
                sys.exit(0)
            # ---------------------------------------------------------------------------------------------------------------


            # ----------------------------- Record file upload metadata in local and NDA databases --------------------------

            #      Assembly meta-data record to be saved to NDA and our local database
            #      Use information from our local spreadsheets, file system, and image files' metadata;
            #      additional info from fast-track NDA database, and NDA data dictionary

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # Calculate or set metadata for current image series, scanning scantype, and processing stage

            # NDA: Data Dictionary: Title:      Processed MRI Data
            # NDA: Data Dictionary: Short Name: fmriresults01

            # NDA requires date format "04/06/2017 00:00:00". This format is set in NDA_db_Metadata_Get()

            # Experiment ID is empty for structural imaging. For fMRI, a number will be provided by the NDA dictionary after we create new experiment types.
            # After all, we decided not to create new experiment types, but to use the existent ids. To see them:
            #   https://ndar.nih.gov/user/dashboard/collections.html
            #   username > Collections > Title = Adolesc... > Experiments

            # Type of scan value must be one of these:
            #   MR diffusion; fMRI; MR structural (MPRAGE); MR structural (T1); MR structural (PD); MR structural (FSPGR); MR structural (FISP); MR structural (T2);
            #   PET; ASL; microscopy; MR structural (PD, T2); MR structural (B0 map); MR structural (B1 map); single-shell DTI; multi-shell DTI; Field Map;
            #   X-Ray; static magnetic field B0
            # There are not values for minimally-processed data, we will use the closest "normal" types. For example, for min-proc T1, exp_scan = 'MR structural (T1)'

            # We are using  session_det  to describe the data processing stage (none = fast-track,  minimally-processed,  processed, ...)
            # and  image_history  to summarize the process

            if scantype == 'MPR':
                exp_id = ''
                exp_scan      = 'MR structural (T1)'
                session_det   = 'ABCD-MPROC-T1'
                image_history = 'gradient unwarp, B1 inhomogeneity correction, resampled to 1mm^3 isotropic in LIA rigid body registration to non-MNI atlas'

            elif scantype == 'XetaT2':
                exp_id = ''
                exp_scan      = 'MR structural (T2)'
                session_det   = 'ABCD-MPROC-T2'
                image_history = 'gradient unwarp, B1 inhomogeneity correction, resampled to 1mm^3 isotropic in LIA rigid body registration to non-MNI atlas'

            elif scantype == 'BOLD':
                exp_id      = NDAexpid_for_modality[modality]
                exp_scan    = 'fMRI'
                session_det = 'ABCD-MPROC-' + bidsufix_for_modality[modality].upper()
                image_history = 'motion correction, B0 inhomogeneity correction, gradient unwarp, between scan motion correction, and resampling to 2.4mm^3 (requires rigid registration to T1 - see included json for matrix values)'

            elif scantype == 'DTI':
                exp_id = ''
                exp_scan      = 'multishell DTI'
                session_det   = 'ABCD-MPROC-DTI'
                image_history = 'eddy-current correction, motion correction, B0 inhomogeneity correction, gradient unwarp, replacement of bad slice-frames, between scan motion correction, rigid body registration to atlas and resampling to 1.7mm^3 LPI (requires rigid registration to T1 - see included json for matrix values)'

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            if nda_fstk_record:
                dataset_id     = '0' if nda_fstk_record['dataset_id'] == ''  else  nda_fstk_record['dataset_id']
                interview_date = nda_fstk_record['interview_date']
                interview_age  = nda_fstk_record['interview_age']
                if 'image_file' in nda_fstk_record:
                    image_file = nda_fstk_record['image_file']
                else:
                    image_file = 's3://nda-abcd/' + FsTk_fname

            else:
                # The associated fast-track data record was not found in NDA.
                # We use then metadata from our local sources: REDCap, Incoming.csv, and fixes.'
                dataset_id = '0'

                # Construct interview date like in anonymizer.sh
                interview_date = '%s/%s/%s' % (series_date[4:6], series_date[6:8], series_date[0:4]) + ' 00:00:00'
                bday = datetime.datetime.strptime( ser_info['dob'], '%Y-%m-%d')
                sday = datetime.datetime.strptime( series_date,  '%Y%m%d')
                interview_age = ("%.1f" % ((sday-bday).days/365.25))
                interview_age = '%.0f' % round( float(interview_age)*12 )

                if TEST_MODE:
                    print('series_date:', series_date )
                    print('bday:', bday )
                    print('sday:', sday )

                image_file = 's3://nda-abcd/' + FsTk_fname

            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

            # Assembly NDA-complying meta-data record (according to specifications in fmriresults01_definitions-2.csv)
            record = {"subjectkey":      pGUID,
                    "src_subject_id":    pGUID,
                    "origin_dataset_id": dataset_id,
                    "interview_date":    interview_date,
                    "interview_age":     interview_age,
                    "gender":            ser_info['gender'],
                    "experiment_id":     exp_id,
                    "inputs":            'ABCD Fast-Track image data release for baseline assessments',
                    "img03_id":          ser_info['nda_id'],   # row_id in image03 data structure, mapping derivative to source record in image03. Recommended
                    "file_source":       image_file,           # Required
                    "job_name":          '',
                    "proc_types":        '',
                    "metric_files":      '',
                    "pipeline":          'MMPS version 248',
                    "pipeline_script":   'MMIL_Preproc',
                    "pipeline_tools":    'MMPS',
                    "pipeline_type":     'MMPS',
                    "pipeline_version":  '248',
                    "qc_fail_quest_reason": '',
                    "qc_outcome":        'pass',
                    "derived_files": AWS_bucket + fname_bas + '.tgz',   # Archive of the files produced by the pipeline. Required
                    "scan_type":     exp_scan,                          # Required
                    "img03_id2":     '',
                    "file_source2":  '',
                    "session_det":   session_det,              # Session details. Recommended
                    "image_history": image_history }
            # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
            # ---------------------------------------------------------------------------------------------------------------


            # ------------------------------ Upload record to miNDA and BIDS strucutre to AWS -------------------------------

            miNDA_ok, miNDA_msg  =  miNDA_record_upload( record )

            print('\nmiNDA_ok =', miNDA_ok)
            print(  'miNDA_msg:', miNDA_msg, '\n')

            if miNDA_ok:
                s3_ok, s3_msg  =  AWS_file_upload( outtarname )
            else:
                # Unable to upload record to miNDA
                s3_ok  = ''
                s3_msg = 'AWS-s3 not attempted because miNDA upload failed'

            print('s3_ok =', s3_ok)
            print('s3_msg:', s3_msg)

            if not miNDA_ok or not s3_ok:
                print('Removing BIDS container:', outtarname )  # So we don't have to remove it manually when re-running the process
                try:
                    os.remove( outtarname )
                except Exception as e:
                    print('Error: unable to remove file', outtarname, e)
            # ---------------------------------------------------------------------------------------------------------------


            # ----------------------------------- Upload record to local SQLite database ------------------------------------
            local_db_table = 'fmriresults01'

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

    print()
# ========================================================================================================================================================





    # if TEST_MODE:
    #     print('NDA_db_Series.shape =', Series.shape )
    # if TEST_MODE:
    #     print('NDA_db_Series:')
    #     print( Series, '\n' )
    # if TEST_MODE:
    #     print('rec:')
    #     print( rec, '\n' )


            # if not 'image_file' in nda_fstk_record:
            #     nda_fstk_record['image_file'] = 's3://nda-abcd/' + FsTk_fname

            # if nda_fstk_record['dataset_id'] == '':
            #     nda_fstk_record['dataset_id'] = '0'

                # dataset_id     = nda_fstk_record['dataset_id']
                # if dataset_id == '':
                #     dataset_id = '0'


    # Year-1 release 0 (2018feb): BIDS_file_create_BOLD:
    # ...
        # "RepetitionTime": 0.8,       
        # "TaskName":  bids_sufix2,
        # "registration_matrix_T1": registration_matrix_read( regis_file )
        # 
        #     Year-1 patch release (2018aug14-):

