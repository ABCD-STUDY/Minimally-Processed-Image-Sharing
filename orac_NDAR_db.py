#!/usr/bin/env python3

#--------------------------------------------------------------------------------------------------------
import sys

if len(sys.argv) < 2 or len(sys.argv) > 3:
    print()
    print('Handles NDA information downloaded into a local oracle data base.')
    print('                                      Octavio Ruiz, 2017nov15-16')
    print('Usage:')
    print('  ./orac_NDAR_db.py  CreateDataBase  (works only when called from run_orac_NDAR_db.sh)')
    print('  ./orac_NDAR_db.py  Duplicates')
    print('  ./orac_NDAR_db.py  GetRecords')
    print('  ./orac_NDAR_db.py  GetRecords   subjectID')
    print('  ./orac_NDAR_db.py  ListFiles    subjectID')
    print('  ./orac_NDAR_db.py  GetID        file_name')
    print('  ./orac_NDAR_db.py  GetIDandRecs file_name')
    print()
    sys.exit()

subject = ''
fname = ''

optn = sys.argv[1]

if optn in ['GetRecords', 'ListFiles'] and len(sys.argv) == 3:
    subject = sys.argv[2]

if optn in ['GetID', 'GetIDandRecs']:
    fname = sys.argv[2]
#--------------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------------
if optn == 'CreateDataBase':

    # Read ABCD fast-track shared data information from NDA, through an oracle miNDA database,
    # and save contents in a .csv file
    #                                               Hauke Bartsch and Octavio Ruiz, 2017nov15

    import cx_Oracle
    import json

    import warnings
    warnings.simplefilter(action='ignore', category=UserWarning)
    warnings.simplefilter(action='ignore', category=FutureWarning)

    import pandas as pd
    pd.set_option('display.width', 512)


    #-----------------------------------------------------------------------
    # Set connection with oracle miNDAR
    config = null
    # todo: make path relative to this scipts path
    with open('config.json') as json_data:
        config = json.load(json_data)
    if config == null:
        print("Error: could not read config file in local directory")
        sys.exit(-1)

    dsnStr = cx_Oracle.makedsn(config['hostname'], config['port'], config['service_name'])
    db = cx_Oracle.connect(user=user,password=pw, dsn=dsnStr)

    # should be relative file name to local script directory
    fname = 'orac_miNDAR.csv'
    print('Reading remote oracle miNDA, and saving contents in file', fname, '...')

    query = """
    select * from image03
    """

    dat = pd.read_sql( query, con=db )

    print( 'dat.shape =', dat.shape )

    dat = dat.sort_values( by=['SUBJECTKEY', 'IMAGE_FILE', 'IMAGE03_ID'] )

    dat.to_csv( fname, index=False )

    print()
    
    sys.exit(0)
    #-----------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------------


#--------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------
def extract_columns( dat ):
    dat = dat[['IMAGE03_ID', 'SUBMISSION_ID', 'COLLECTION_ID', 'DATASET_ID', 'SUBJECTKEY', 'INTERVIEW_DATE',
               'IMAGE_FILE',
               'IMAGE_DESCRIPTION',
               'EXPERIMENT_ID',
               'SCANNER_MANUFACTURER_PD', 'SCANNER_TYPE_PD',
               'VISIT']]
    return dat
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
def dat_dups( subjs, varX, vary ):
    dups_num  = -1
    dups      = []
    vary_list = []
    subjs_N_per_vary = []
    # 
    subjs = subjs.sort_values( ['IMAGE_FILE'], ascending=True )
    subjs = subjs.reset_index(drop=True)
    # 
    mask = subjs.duplicated( varX )
    dups_num = mask.sum()
    # 
    if dups_num > 0:
        # Find rows with duplicated varXs
        mask2 = mask | mask.shift(-1)
        dups = subjs.loc[mask2]
        # 
        # List of varys associated with duplicated varXs
        dups_vary_list = subjs.loc[mask2][vary].unique().tolist()
        # 
    return dups_num, dups, dups_vary_list
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
pd.set_option('display.width', 512)
#-----------------------------------------------------------------------


#-----------------------------------------------------------------------
db_name = 'orac_miNDAR.csv'

dat = pd.read_csv( db_name )


if optn == 'Duplicates':
    print('--------------------------------------------------------------------------------')
    print( 'dat.shape =', dat.shape )
    # # For tests
    # dat = dat[ dat['SUBJECTKEY'] == 'NDAR_INV007W6H7B' ]
    # print( 'dat.shape =', dat.shape )

    dups_num, dups, mult_subj_IDs  =  dat_dups( dat, ['IMAGE_FILE'], 'IMAGE03_ID' )

    print()
    print('Check for rows which FILE_NAME appears in a previous row, and that have different IMAGE03_IDs')
    print()
    print('Number of repeated FILE_NAMEs with different IMAGE03_IDs: ', dups_num )
    print()
    dups = extract_columns( dups )
    print( 'Cases exhibiting duplicated FILE_NAMEs:\n', dups )
    print()
    print('--------------------------------------------------------------------------------')
    sys.exit()


if optn == 'GetRecords':
    if len(subject) > 0:
        dat = dat[ dat['SUBJECTKEY'] == subject ]

    dat = extract_columns( dat )
    print( dat )
    print()


if optn == 'ListFiles':
    # Return list of files in NDAR belonging to given subject
    dat = dat[ dat['SUBJECTKEY'] == subject ]

    files = [ s.rsplit('/')[-1] for s in dat['IMAGE_FILE'] ]

    print( files )
    print()
    sys.exit(0)


if optn == 'GetIDandRecs':   # Return records containing given file name
    dat = dat[ [fname == s.rsplit('/')[-1]  for s in dat['IMAGE_FILE']] ]

    dat = extract_columns( dat )

    print( dat )
    print()
    sys.exit(0)


if optn == 'GetID':   # Return NDAR ID corresponding to given file name
    dat = dat[ [fname == s.rsplit('/')[-1]  for s in dat['IMAGE_FILE']] ]

    for rec in dat['IMAGE03_ID'].tolist():
        print( rec )
    sys.exit(0)

#--------------------------------------------------------------------------------------------------------


# >>> dat.columns
# Index(['IMAGE03_ID', 'SUBMISSION_ID', 'COLLECTION_ID', 'DATASET_ID', 'SUBJECTKEY', 'SRC_SUBJECT_ID', 'INTERVIEW_DATE', 'INTERVIEW_AGE', 'GENDER', 'COMMENTS_MISC', 'IMAGE_FILE', 'IMAGE_THUMBNAIL_FILE', 'IMAGE_DESCRIPTION', 'EXPERIMENT_ID', 'SCAN_TYPE', 'SCAN_OBJECT', 'IMAGE_FILE_FORMAT', 'DATA_FILE2', 'DATA_FILE2_TYPE', 'IMAGE_MODALITY', 'SCANNER_MANUFACTURER_PD', 'SCANNER_TYPE_PD', 'SCANNER_SOFTWARE_VERSIONS_PD', 'MAGNETIC_FIELD_STRENGTH', 'MRI_REPETITION_TIME_PD', 'MRI_ECHO_TIME_PD', 'FLIP_ANGLE',
#        'ACQUISITION_MATRIX', 'MRI_FIELD_OF_VIEW_PD', 'PATIENT_POSITION', 'PHOTOMET_INTERPRET', 'RECEIVE_COIL', 'TRANSMIT_COIL', 'TRANSFORMATION_PERFORMED', 'TRANSFORMATION_TYPE', 'IMAGE_HISTORY', 'IMAGE_NUM_DIMENSIONS', 'IMAGE_EXTENT1', 'IMAGE_EXTENT2', 'IMAGE_EXTENT3', 'IMAGE_EXTENT4', 'EXTENT4_TYPE', 'IMAGE_EXTENT5', 'EXTENT5_TYPE', 'IMAGE_UNIT1', 'IMAGE_UNIT2', 'IMAGE_UNIT3', 'IMAGE_UNIT4', 'IMAGE_UNIT5', 'IMAGE_RESOLUTION1', 'IMAGE_RESOLUTION2', 'IMAGE_RESOLUTION3', 'IMAGE_RESOLUTION4',
#        'IMAGE_RESOLUTION5', 'IMAGE_SLICE_THICKNESS', 'IMAGE_ORIENTATION', 'QC_OUTCOME', 'QC_DESCRIPTION', 'QC_FAIL_QUEST_REASON', 'DECAY_CORRECTION', 'FRAME_END_TIMES', 'FRAME_END_UNIT', 'FRAME_START_TIMES', 'FRAME_START_UNIT', 'PET_ISOTOPE', 'PET_TRACER', 'TIME_DIFF_INJECT_TO_IMAGE', 'TIME_DIFF_UNITS', 'PULSE_SEQ', 'SLICE_ACQUISITION', 'SOFTWARE_PREPROC', 'STUDY', 'WEEK', 'EXPERIMENT_DESCRIPTION', 'VISIT', 'SLICE_TIMING', 'BVEK_BVAL_FILES', 'BVECFILE', 'BVALFILE', 'DEVICESERIALNUMBER', 'PROCDATE',
#        'VISNUM', 'COLLECTION_TITLE', 'PROMOTED_SUBJECTKEY'],
#       dtype='object')
