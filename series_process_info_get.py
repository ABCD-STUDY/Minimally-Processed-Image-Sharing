#!/usr/bin/env python3

import sys, os

from scipy.io import loadmat

import warnings
warnings.simplefilter(action='ignore', category=UserWarning)
import pandas as pd
pd.set_option('display.width', 512)
pd.set_option('max_colwidth', 60)

import glob, json, time

#------------------------------------------------------------------------------------------------------------------------------------------
Dirs_Loc_fname = '/home/abcdproc1/ProjInfo/MMIL_ProjInfo.csv'
PCInfo_fname   = '/home/abcdproc1/MetaData/DAL_ABCD/DAL_ABCD_pcinfo.csv'

filt = {'DTI_ndiffdirs_min':  50,   # Don, 2018aug09,10.  Before it was thresh = 0
        'BOLD_nreps_min':    100    # Don, 2018jan__
        }
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

modality_list = ['T1',    'T2',   'dMRI', 'fMRI_MID_task', 'fMRI_SST_task', 'fMRI_nBack_task', 'rsfMRI']
scantype_list = ['MPR', 'XetaT2',  'DTI',      'BOLD',          'BOLD',          'BOLD',         'BOLD']
task_list     = [  '',     '',       '',       'MID',           'SST',           'nBack',          ''  ]
fstktype_list = ['T1',    'T2',    'DTI',      'MID-fMRI',      'SST-fMRI',      'nBack-fMRI', 'rsfMRI']
task_for_modality     = dict( zip( modality_list, task_list) )
fstktype_for_modality = dict( zip( modality_list, fstktype_list) )

addit_var_list = ['ndiffdirs', 'nreps', 'TR', 'TE', 'FlipAngle']   # 'TI' exists only for T1 series; it is handled in the code, below

Verbose = False    # Set through command line
#------------------------------------------------------------------------------------------------------------------------------------------


# ========================================================================================================================================================
#----------------------------------------------------------------------------------------
def program_description():
    print()
    print('Locate processed image-series directories, files, and associated fast-track data for a given ABCD participant and MRI/fMRI modality')
    print('Gets global location of processed files from:', Dirs_Loc_fname )
    print('Gets series and task information from:       ', PCInfo_fname )
    print('Gets process info from the participant-task   ContainerInfo.mat')
    print('Series are selected based on rules programed in this script, using parameters:')
    print( json.dumps( filt, sort_keys=True, indent=2 ) )
    print('                                                                                    Octavio Ruiz,  2017nov20-2018jan18, jul30-aug23')
    print()
    print('Usage within a Python3 script:')
    print('    from series_process_info_get import Get_File_Names_and_Process_Info')
    print('    ...')
    print('    Files  =  Get_File_Names_and_Process_Info( subj, modality )')
    print('    ...\n')
    print()
    print('Stand-alone usage:')
    print('  ./series_process_info_get.py  Subject  Modality  option')
    print()
    print('  where:')
    print('    Subject    Subject ID (without any "NDAR" or "NDAR_" prefix)' )
    print('    Modality   One of:', modality_list )
    print('    option     -v => Verbose' )
    print()
    print("Returns a dictionary containing one dictionary per run; where each run contains:")
    print('      Run-01')
    print('         MinProc_file               Minimally-processed image series')
    print('         FasTrk_file                Associated data file name in our local /fast-track')
    print('         FasTrk_file_nopath         idem, without path')
    print('         FasTrk_file_Guessed_Name   What should be the fast-track fname, constructed from subject, modality, and series date&time')
    print('         FlipAngle, TR, TE')
    print('         series_date                Series date')
    print('         series_time                Series time')
    print('      Run-02 ...                    etc, if more than one series exist')
    print()
    print("Additionaly, depending on modality:")
    print()
    print('  T1 or T2:')
    print('      Run-01')
    print('         MinProc_file         Minimally-processed series file: MPR_res.mgz or T2w_res.mgz, respectively,')
    print('                              and with "NORM" in the path name, if present')
    print('         TI                   Inversion time; only present when Modality = T1')
    print()
    print('  fMRI_MID_task, fMRI_SST_task, fMRI_nBack_task, rsfMRI:')
    print('      Run-01')
    print('         Event_file           Events')
    print('         Motion_file          Motion-correction')
    print('         Regis_file           Registration')
    print('         nreps                Number of repetitions')
    print('      Run-02 ...              etc, if more than one series exist')
    print()
    print('  dMRI:')
    print('      Run-01')
    print('         RegMtx_file          Path to registration-matrix file: *_corr_regT1_regT1.mat  with no "rev"')
    print('         RegistrationMatrix   M_T1_to_T2')
    print('         bval_file            Path to b-values file:  bvals.txt')
    print('         bvec_file            Path to b-vectors file: bvecs.txt')
    print('         ndiffdirs            Number of different directions')
    print('         nreps                Number of repetitions')
    print('      Run-02 ...              etc, if more than one series exist')
    print()
    print('For more information about TR, TE, TI, FlipAngle see: https://abcdstudy.org/images/Protocol_Imaging_Sequences.pdf')
    print()
    print('  Examples:')
    print('    ./series_process_info_get.py  INV028D3ELL  T1  -v')
    print('    ./series_process_info_get.py  INV028D3ELL  fMRI_MID_task  -v')
    print('    ./series_process_info_get.py  INV028D3ELL  rsfMRI  |  jq')
    print('    ./series_process_info_get.py  INV9KT9V114  fMRI_SST_task  -v')
    print('    ./series_process_info_get.py  INVWRFE4X5R  dMRI  |  jq')
    print()
#----------------------------------------------------------------------------------------

#----------------------------------------------------------------------------------------
def command_line_get_variables():
    subj     = ''
    modality = ''
    verbose  = False

    if len(sys.argv) < 3 or len(sys.argv) > 4:
        program_description()
        sys.exit()

    subj     = sys.argv[1]
    modality = sys.argv[2]
    
    if len(sys.argv) == 4  and  'v' in sys.argv[3]:
        verbose = True

    if modality not in modality_list:
        print('Error: Modality must be one of', modality_list )
        sys.exit()
        
    return subj, modality, verbose
#------------------------------------------------------------------------------------------------------------------------------
# ========================================================================================================================================================



# ========================================================================================================================================================
# ---------------------------------------------------------------------------------------------------------------
def PCInfo_get( subj, modality ):
    # Locate subject in /home/abcddaic/MetaData/DAL_ABCD_QC/DAL_ABCD_QC_combined_pcinfo.csv,
    # get rows with SeriesType == modality,
    # and extract  SeriesInstanceUID.

    PCInfo = pd.DataFrame()

    # print('PCInfo.columns:', PCInfo.columns )
    # ['pGUID', 'VisitID', 'EventName', 'SessionType', 'SiteName', 'SeriesType', 'ABCD_Compliant', 'SeriesDescription',
    # 'Completed', 'AdditionalInfo', 'NumberOfFiles', 'ImagesInAcquisition', 'AcquisitionTime', 'NumberOfTemporalPositions',
    # 'AcquisitionMatrix', 'Rows', 'PercentPhaseFieldOfView', 'NumberOfPhaseEncodingSteps', 'RepetitionTime', 'EchoTime', 'SeriesNumber',
    # 'Manufacturer', 'SequenceName', 'ImageType', 'PatientID', 'PatientFamilyName', 'StudyInstanceUID', 'SeriesInstanceUID', 'StudyDate',
    # 'StudyTime', 'SeriesTime', 'version', 'SiteID', 'PixelBandwidth', 'Channel', 'CoilType', 'fname_json', 'fname_pc_json', 'StudyInstanceUID_SeriesTime']
    # 
    # print('SeriesTypes:')
    # print( PCInfo['SeriesType'].unique() )
    # ['T1', 'T2',
    #  'dMRI', 'dMRI_FM_AP', 'dMRI_FM_PA', 'fMRI_FM_AP', 'fMRI_FM_PA',
    #  'fMRI_MID_task', 'fMRI_SST_task', 'fMRI_nBack_task', 'rsfMRI']

    start_time = time.time()
    # # 2017:
    # PCInfo  =  pd.read_csv( '/home/abcddaic/MetaData/DAL_ABCD_QC/DAL_ABCD_QC_combined_pcinfo.csv', low_memory=False )
    # PCInfo = PCInfo[['pGUID', 'EventName', 'SiteName', 'SeriesType',
    #                  'SeriesInstanceUID', 'StudyDate', 'SeriesTime']]
    # 2017 patch:
    PCInfo  =  pd.read_csv( PCInfo_fname, low_memory=False,
                            usecols=['pGUID', 'EventName', 'SiteName', 'Manufacturer',
                                      'SeriesType', 'SeriesInstanceUID', 'StudyDate', 'SeriesTime'] )
    elapsed_time = time.time() - start_time

    if Verbose:
        print('PCInfo.shape:', PCInfo.shape )
        print('Reading time: %.1f s' % elapsed_time )
        print()

    mask = [ subj in s  for s in PCInfo['pGUID'] ]
    PCInfo = PCInfo.loc[mask]

    if Verbose:
        print('PCInfo:')
        print( PCInfo )
        print('PCInfo.shape:', PCInfo.shape )
        print('SeriesTypes:')
        print( PCInfo['SeriesType'].unique() )
        print()

    if modality == 'T1':
        mask = [ s in ['T1', 'T1_NORM'] for s in PCInfo['SeriesType'] ]
        PCInfo = PCInfo.loc[mask]
    elif modality == 'T2':
        mask = [ s in ['T2', 'T2_NORM'] for s in PCInfo['SeriesType'] ]
        PCInfo = PCInfo.loc[mask]
    else:
        PCInfo = PCInfo[ PCInfo['SeriesType'] == modality ]
    # PCInfo = PCInfo[ PCInfo['SeriesType'] == modality ]

    PCInfo = PCInfo.sort_values( ['StudyDate', 'SeriesTime'], ascending=True ).reset_index(drop=True)
    PCInfo.index = PCInfo.index+1

    # Create new column, with time order, to be used when locating events file
    PCInfo['t_ord'] = range(1, len(PCInfo)+1)

    if Verbose:
        print('PCInfo:')
        print( PCInfo )
        print('PCInfo.shape:', PCInfo.shape )
        print()

    return PCInfo
# ---------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------
def Sers_filter_by_UID( subj, SerInfo_this_process, modality ):
    # Get series information from DAL_ABCD_QC_combined_pcinfo
    PCInfo = PCInfo_get( subj, modality )

    # Use this information to filter a subset of the table SerInfo, derived from ContainerInfo
    NewTable = SerInfo_this_process.copy()

    # Specify two origins of SeriesType information
    NewTable = NewTable.rename( columns={'SeriesType': 'ciSeriesType'} )

    NewTable['scan'] = NewTable.index
    NewTable = NewTable.merge( PCInfo, on='SeriesInstanceUID' )
    NewTable = NewTable.set_index('scan', drop=True)

    if all([float(s) for s in NewTable['SeriesTime_x']] == NewTable['SeriesTime_y']):
        NewTable = NewTable.drop('SeriesTime_y', axis='columns')
        NewTable = NewTable.rename( columns={'SeriesTime_x': 'SeriesTime'} )
    else:
        acceptable = False
        if Verbose:
            print("Error: SeriesTime in ContainerInfo and PCInfo differ")
            print( [float(s) for s in NewTable['SeriesTime_x']] == NewTable['SeriesTime_y']  )
        sys.exit()


    # Check if all identified series in pcinfo exist in ContainerInfo:
    if len(NewTable) != len(PCInfo):
        if Verbose:
            print('Warning: Number of selected ContainerInfo series different from selected PCInfo series')

    # If there are more than 2 series in new table, we will not share this process for now
    if len(NewTable) > 2:
        acceptable = False
        if Verbose:
            print('Warning: Number of selected series is larger than 2; we will not share this process')

    return NewTable
# ---------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------
def Sers_from_ContainerInfo_and_PCinfo( subj, modality, scantype, fpath ):

    path_to_search  =  fpath + 'PROC*_' + subj + '_*'

    if Verbose:
        print('Min.Processed data: path_to_search:')
        print( path_to_search )

    f_list = glob.glob( path_to_search )

    # Remove non-directories from file list
    dir_n = 0
    non_dir_n = 0
    for f in f_list:
        if os.path.isfile( f ):
            non_dir_n += 1
            f_list.remove(f)
    dir_n = len(f_list)

    if Verbose:
        print('Found %.0f files (ignored) and %.0f directories:' % (non_dir_n, dir_n) )
        print( '\n'.join(f_list) )

    if len(f_list) == 0:
        print("Error: unable to find minimally-processed directory for requested subject")
        sys.exit()

    elif len(f_list) > 1:
        print("Error: found too many min.processed-series directories; cannot continue")
        sys.exit()

    fdir = f_list[0]
    #----------------------------------------------------------------------------------------


    #----------------------------------------------------------------------------------------
    #       ContainerInfo.mat :  Read Matlab file containing structure ContainerInfo

    fname = 'ContainerInfo.mat'
    fname = fdir + '/' + fname
    try:
        data = loadmat(fname, squeeze_me=True, struct_as_record=True)
    except:
        print('Error: unable to read', fname )
        sys.exit()

    # Extract structure of interest
    CntrInfo = data['ContainerInfo']

    # Fields in CntrInfo depend on modality:
    # print()
    # print('Fields in CntrInfo:', CntrInfo.dtype.names)
    # print()
    #                      'SourceDir', 'ContainerType', 'ContainerUID', 'ContainerCreationDate', 'VisitID', 'StudyDate', 'StudyTime',
    #    'StudyInstanceUID', 'MagneticFieldStrength', 'SeriesInfo', 'Manufacturer', 'ManufacturersModelName', 'BOLD_cntr', 'FMAP_cntr',
    # 'ScanInfo', 'Updated', 'ProjID', 'MMPSVER'
    # 
    # BOLD:
    # Fields in CntrInfo: ('SourceDir', 'ContainerType', 'ContainerUID', 'ContainerCreationDate', 'VisitID', 'StudyDate', 'StudyTime',
    #    'StudyInstanceUID', 'MagneticFieldStrength', 'SeriesInfo', 'Manufacturer', 'ManufacturersModelName', 'BOLD_cntr', 'FMAP_cntr',
    #    'ScanInfo', 'Updated', 'ProjID', 'MMPSVER', 'MMPS_nonsvn')
    # DTI:
    # Fields in CntrInfo: ('SourceDir', 'ContainerType', 'ContainerUID', 'ContainerCreationDate', 'VisitID', 'StudyDate', 'StudyTime',
    #    'StudyInstanceUID', 'MagneticFieldStrength', 'SeriesInfo', 'Manufacturer', 'ManufacturersModelName', 'DTI_cntr', 'FMAP_cntr',
    #    'ScanInfo', 'Updated', 'ProjID', 'MMPSVER', 'MMPS_nonsvn')

    manuf = CntrInfo['Manufacturer'].item()
    #----------------------------------------------------------------------------------------

    #----------------------------------------------------------------------------------------
    #       ContainerInfo.mat :  Extract SeriesInfo into a Pandas data frame

    # Access structure array
    SerInfo = CntrInfo['SeriesInfo']
    SerInfo = SerInfo.tolist()
    # Recreate structure as a Pandas data frame
    SerInfo = pd.DataFrame( SerInfo, columns=SerInfo.dtype.names )
    # Reindex from 0... to 1..., in order to make table comparable with Matlab and direclty usable with file naming
    SerInfo.index = SerInfo.index+1

    # if Verbose:
    #     print('\n                  SerInfo: first two records:\n')
    #     print(SerInfo.iloc[0], '\n')
    #     print(SerInfo.iloc[1], '\n')

    PatientID = SerInfo['PatientID'].unique()
    PatientID = PatientID.tolist()

    ContainerType = CntrInfo['ContainerType'].item()

    if Verbose:
        print()
        if len(PatientID) > 1:
            print('Warning: more than one PatientID in ContainerInfo series')
        print('PatientID:    ', PatientID )
        print('ContainerType:', ContainerType )
        print('Manufacturer: ', manuf )
        # print()
        # print("SerInfo.columns:")
        # print( SerInfo.columns.tolist() )

    # Extract ContainerInfo.SeriesInfo(s).info.Private_2001_101b, that must be the ITs for each series
    SIi    = SerInfo['info']
    SIi_l  = SIi.tolist()
    # SIi_l0 = SIi_l[0]
    # TI = SIi_l0['Private_2001_101b'].item()
    TIpriv = []
    for s in range(0,len(SIi_l)):
        try:
            TIpriv.append( SIi_l[s]['Private_2001_101b'].item() )
        except:
            TIpriv.append( float('nan') )

    # Keep columns of interest per series
    SerInfo = SerInfo[['SeriesNumber','SeriesType', 'SeriesDescription', 'SeriesInstanceUID', 'SeriesDate', 'SeriesTime', 'Manufacturer']]

    if Verbose:
        print('SerInfo:')
        print( SerInfo )
    #----------------------------------------------------------------------------------------

    #----------------------------------------------------------------------------------------
    #       ContainerInfo.mat :  Extract ScanInfo and incorporate its contents to main table

    SerInfo_this_process = pd.DataFrame()

    ScanInfo = CntrInfo['ScanInfo'].tolist()
    # if Verbose:
    #     print()
    #     print('Fields in structure ScanInfo:', ScanInfo.dtype.names)

    if modality == 'T1':
        var_list = addit_var_list + ['TI']
    else:
        var_list = addit_var_list

    st_num = 0
    inds = []
    for st in ScanInfo.dtype.names:
        if st in ['MPR', 'XetaT2', 'BOLD', 'DTI']:
            st_num += 1
            scan = ScanInfo[st].item()
            if Verbose:
                print()
            #     print('st =', st )
            #     # print('scan = ScanInfo[st].item() =')
            #     # print( scan )
            #     # print('type(scan) =', type(scan) )
            #     # print('scan.size =', scan.size )
                print('scan.dtype.names:', scan.dtype.names )
            if scan.size > 0:
                # Extract additional variables, if present
                scSI = scan['SeriesIndex'].flatten().tolist()
                # if Verbose:
                #     print("scan['SeriesIndex'] = ", end='') ;   print( scSI )
                for var in var_list:
                    if var in scan.dtype.names:
                        scVals = scan[var].flatten().tolist()
                        # if Verbose:
                        #     print("appending scan['%s']: " % var, end='' ) ;   print( scVals )
                        SerInfo.loc[ scSI, var ] = scVals
                inds += scSI

    # Append new column with TIs extracted from ContainerInfo.SeriesInfo(s).info.Private_2001_101b
    SerInfo['TIpriv'] = TIpriv

    if Verbose:
        print()
        print('There are %.0f acceptable MRI_types in ContainerInfo' % st_num, ':  inds =', inds )

    if st_num <= 0:
        if Verbose:
            print()
            print('Error: ContainerInfo does not include any acceptable MRI_types')
        sys.exit()

    if Verbose:
        print()
        print('SerInfo:')
        print( SerInfo )

    SerInfo_this_process = SerInfo.iloc[ [s-1 for s in inds] ]

    if Verbose:
        print()
        print('SerInfo_this_process:')
        print( SerInfo_this_process )

    SerInfo_this_process = SerInfo_this_process.reset_index(drop=True)
    SerInfo_this_process.index = SerInfo_this_process.index+1
    #----------------------------------------------------------------------------------------

    #----------------------------------------------------------------------------------------
    if Verbose:
        print()
        print('scantype =', scantype )

    if scantype in ['MPR', 'XetaT2']:
        Series = SerInfo_this_process[ SerInfo_this_process['SeriesType'] == scantype ]

    elif scantype == 'DTI':
        Series = SerInfo_this_process[ ([scantype in s for s in SerInfo_this_process['SeriesType']])  &  (SerInfo_this_process['ndiffdirs'] >= filt['DTI_ndiffdirs_min']) ]

    elif scantype == 'BOLD':
        Series = SerInfo_this_process[ SerInfo_this_process['nreps'] >= filt['BOLD_nreps_min'] ]

    else:
        print('Error: unrecognized scantype:', scantype )
        sys.exit()


    if Verbose:
        print()
        if not len(Series):
            print('No suitable series found in ContainerInfo \n')
        else:
            print('Series (filtered):')
            print( Series, '\n')

    # Combine with series recorded in /home/abcddaic/MetaData/DAL_ABCD_QC/DAL_ABCD_QC_combined_pcinfo.csv, merging by SeriesInstanceUID.
    # So we know t_ord and event, necessary to locate or construct the corresponding fast-track file
    Series  =  Sers_filter_by_UID( subj, Series, modality )
    #----------------------------------------------------------------------------------------

    return Series, fdir, manuf

# End of Sers_from_ContainerInfo_and_PCinfo
# ---------------------------------------------------------------------------------------------------------------



# ---------------------------------------------------------------------------------------------------------------
def T1T2_file_names_get( fdir, scantype, scan_number ):
    Proc_files = {}

    if scantype == 'MPR':
        path_to_search  =  fdir + '/MPR_res.mgz*'
    elif scantype == 'XetaT2':
        path_to_search  =  fdir + '/T2w_res.mgz*'

    for sn in scan_number:
        res_f_list = glob.glob(path_to_search)
        if Verbose:
            print( path_to_search )
            print( '\n'.join(res_f_list) )

    if len(res_f_list) == 0:
        print("Error: unable to find minimally-processed data for requested subject")
        sys.exit()

    if len(res_f_list) > 1:
        print("Error: found too many min.processed-series; I don't know what to do")
        sys.exit()

    # Construct a dictionary with the resolved names of processed-data files to share
    Proc_files = {'Run-01': {'MinProc_file': res_f_list[0]} }

    # keys  =  [ 'MinProc_file%.0f'%(j+1)  for  j in range(0, len(res_f_list)) ]
    # Proc_files = dict(zip(keys,res_f_list))

    return Proc_files
# ---------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------
def File_names_filter( f_list, key, infix ):
    uFiles = {}

    sub_list = [s  for s in f_list  if infix in s]

    if len(sub_list) == 0:
        uFiles.update( dict( {key: ''} ) )
    elif len(sub_list) == 1:
        uFiles.update( dict( {key: sub_list[0]} ) )
    else:
        uFiles.update( dict( {key: ''} ) )
        print("Error: found too many", infix, "files under subdir; I don't know what to do")
        sys.exit()

    return uFiles
# ---------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------
def BOLD_file_names_get( fdir, scan_number, task, t_ord ):

    # Locate files with names
    #   ...BOLDn_for_corr_resBOLD.mgz
    #   ...BOLDn_for_corr_resBOLD_motion.1D
    #   ...BOLDm_for_corr_resBOLD_regT1.mat
    # and assembly dictionary with results
    Proc_files = {}

    if Verbose:
        print()
        print('Process and results files: path_to_search, and found files:' )

    # --------------------------------------------------------------------------------------
    # Locate registration-matrix file; one for all scans.
    # There should be only one in subdir, and its number is independent of those in scan_number
    # Don suggested to search for it by matching "BOLD*for*corr_resBOLD_regT1.mat", I prefered to be explicit.

    key = 'Regis_file'
    
    path_to_search  =  fdir + '/BOLD' + '*' + '_for_corr_resBOLD_regT1.mat'
    f_list = glob.glob(path_to_search)

    if Verbose:
        print( path_to_search )
        print('\n'.join(f_list) )
        print()

    if len(f_list) > 0:
        RegisFile = File_names_filter( f_list, key, '_for_corr_resBOLD_regT1.mat' )
        
    else:
        path_to_search  =  fdir + '/BOLD' + '*' + '_for_f0_corr_resBOLD_regT1.mat'
        f_list = glob.glob(path_to_search)

        if Verbose:
            print( path_to_search )
            print('\n'.join(f_list) )
            print()

        RegisFile = File_names_filter( f_list, key, '_for_f0_corr_resBOLD_regT1.mat' )
    # --------------------------------------------------------------------------------------


    # --------------------------------------------------------------------------------------
    # Locate image and motion files, one per scan
    for j, sn in enumerate(scan_number):
        uFiles = {}

        # path_to_search  =  fdir + '/BOLD' + sn + '*'
        path_to_search  =  fdir + '/BOLD%.0f*' % sn

        f_list = glob.glob(path_to_search)

        if Verbose:
            print( path_to_search )
            print('\n'.join(f_list) )
            print()

        # Find image and motion-correction files
        key = 'MinProc_file'
        uFiles.update( File_names_filter( f_list, key, '_for_corr_resBOLD.mgz' ) )

        key = 'Motion_file'
        uFiles.update( File_names_filter( f_list, key, '_for_corr_resBOLD_motion.1D' ) )

        uFiles.update( RegisFile )

        # ---------- Locate BIDS events file ----------------------------------
        if task in ['MID', 'SST', 'nBack']:
            # path_to_search  =  fdir + '/stim*%s/%s*%.0f_events.tsv' % (task, task, j+1)

            path_to_search  =  fdir + '/stim*%s/%s*%.0f_events.tsv' % (task, task, t_ord[j])

            f_list = glob.glob(path_to_search)

            if Verbose:
                print( path_to_search )
                print('\n'.join(f_list) )
                print()

            key = 'Event_file'
            if len(f_list):
                uFiles.update( dict( {key: f_list[0]} ) )
            else:
                uFiles.update( dict( {key: ''} ) )
        # ---------------------------------------------------------------------

        # Incorporate this series' files into output dictionary. Use time order to set the BIDS run number
        # key = 'Run-%02.0f'%(j+1)

        key = 'Run-%02.0f' % t_ord[j]

        Proc_files.update( dict( {key: uFiles} ) )
    # --------------------------------------------------------------------------------------

    return Proc_files
# ---------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------
def DTI_file_names_and_RegMtx_get( fdir, manuf, scan_number ):
    # Locate files containing:
    #   Image results:  ...nii.gz
    #   Diffusion parameters:  bvals.txt, bvecs.txt
    #   and Registration matrix:  look for a  _corr_regT1_regT1.mat  with no "rev"
    # Return dictionary with results
    Proc_files = {}

    # -----------------------------------------------------------------------------------------------
    res_f_list = []

    for j,sn in enumerate(scan_number):

        uFiles = {}

        path_to_search  =  fdir + '/exportDTIforFSL/DTI%.0f/*' % sn
        f_list = glob.glob(path_to_search)

        if Verbose:
            print( path_to_search )
            print('\n'.join(f_list) )
            print()

        # -----------------------------------------------------------------------------------------------
        #                                    Find image file(s)

        res_f_list = [s  for s in f_list  if '.nii.gz' in s]
        if len(res_f_list) > 1:
            print("Error: found too many nIfTI DTI files under subdir; I don't know what to do")
            sys.exit()

        key = 'MinProc_file'
        uFiles.update( dict( {key: res_f_list[0]} ) )
        # -----------------------------------------------------------------------------------------------

        # -----------------------------------------------------------------------------------------------
        #                                Find bval and vector files

        bval_f_list = [s  for s in f_list  if 'bvals' in s]
        if len(bval_f_list) > 1:
            print("Error: found too many bval DTI files under subdir; I don't know what to do")
            sys.exit()

        bvec_f_list = [s  for s in f_list  if 'bvecs' in s]
        if len(bvec_f_list) > 1:
            print("Error: found too many bvec DTI files under subdir; I don't know what to do")
            sys.exit()

        key = 'bval_file'
        uFiles.update( dict( {key: bval_f_list[0]} ) )

        key = 'bvec_file'
        uFiles.update( dict( {key: bvec_f_list[0]} ) )
        # -----------------------------------------------------------------------------------------------

        # -----------------------------------------------------------------------------------------------
        #                            Registration matrix: file and values

        # Look for a  _corr_regT1_regT1.mat  with no "rev",
        # extract  RegInfo.M_T1_to_T2 –> registration matrix for T1 to DTI

        path_to_search  =  fdir + '/*_corr_regT1_regT1.mat'
        regmtx_f_list = glob.glob(path_to_search)
        regmtx_f_list = [s  for s in regmtx_f_list  if 'rev' not in s]

        if Verbose:
            print( path_to_search )
            print('\n'.join(f_list) )
            print()

        if len(regmtx_f_list) <= 0:
            print("Error: unable to find DTI registration-matrix file")
            sys.exit()

        if len(regmtx_f_list) > 1:
            print("Warning: found more than one DTI registration-matrix files under subdir; taking first one")

        regmtx_f = regmtx_f_list[0]

        key = 'RegMtx_file'
        uFiles.update( dict( {key: regmtx_f} ) )

        # Read registration matrix from container and append it to output dictionary
        RegMtx = {}
        key = 'RegistrationMatrix'
        uFiles.update( dict( {key: [] } ) )

        try:
            data = loadmat(regmtx_f, squeeze_me=True, struct_as_record=True)

            Info = data['RegInfo']

            # Read matrix as a Python ndarray
            RegMtx = Info['M_T1_to_T2'].item()

            uFiles.update( dict( {key: RegMtx.tolist() }))

        except:
            print('Warning: unable to read registration matrix', regmtx_f )

        if Verbose:
            print('RegMtx:')
            print( RegMtx )
            print()
        # -----------------------------------------------------------------------------------------------

        # -----------------------------------------------------------------------------------------------
        #                 Incorporate current-series information to output dictionary

        key = 'Run-%02.0f'%(j+1)
        Proc_files.update( dict( {key: uFiles} ) )
        # -----------------------------------------------------------------------------------------------
    # -----------------------------------------------------------------------------------------------

    # -----------------------------------------------------------------------------------------------
    if 'GE' in manuf.upper():
        f_n_max = 1
    elif 'PH' in manuf.upper():
        f_n_max = 2
    elif 'SI' in manuf.upper():
        f_n_max = 1

    if len(res_f_list) == 0:
        print("Error: unable to find minimally-processed data for requested subject")
        sys.exit()
    if len(res_f_list) > f_n_max:
        print("Error: found too many min.processed-series; I don't know what to do")
        sys.exit()
    # -----------------------------------------------------------------------------------------------

    # # TEST
    # if Verbose:
    #     print('Proc_files:')
    #     print( json.dumps( Proc_files, sort_keys=True, indent=2 ) )

    return Proc_files

# End of DTI_file_names_and_RegMtx_get    
# ---------------------------------------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------------------------------------
def FasTrk_files_names_get( Series, scan_number, subj, modality ):
    # Find corresponding /fast-track files according to series time
    # DTIs may and BOLDs have more than one run of the same task.
    FasTrk_files = {}
    
    t_ord = Series['t_ord'].tolist()

    if Verbose:
        print()
        print('Fast-track: path_to_search, and found files:' )

    for j,sn in enumerate(scan_number):
        uFiles = {}

        series_date = Series['SeriesDate'][sn]
        series_date = series_date.split('.')[0]
        series_time = Series['SeriesTime'][sn]
        series_time = series_time.split('.')[0]
        event       = Series['EventName'][sn]

        path_to_search  =  '/fast-track/*/NDAR' + subj + '_*' + series_time + '*'
        if Verbose:
            print( path_to_search )

        f_list = glob.glob( path_to_search )

        if Verbose:
            print( '\n'.join(f_list) )


        # If structural data: pick fast-track file that has "NORM" in its name, if it exists
        if modality in ['T1', 'T2']:
            new_f_list  =  [s for s in f_list  if 'NORM' in s]
            if len(new_f_list) > 0:
                f_list = new_f_list
            if Verbose:
                print('Filtered file list:')
                print( '\n'.join(f_list) )


        # Check number of located files
        if len(f_list) > 1:
            if Verbose:
                print("Error: found too many fast-track files; I don't know what to do")
            return {}

        if len(f_list) == 0:
            f_list = ['']
            if Verbose:
                print("Unable to find corresponding fast-track file for requested subject and series type")


        # Construct what should be the fast-track file name; of the form: NDARINVWAC9RH98_baselineYear1Arm1_ABCD-DTI_20170504173816.tgz
        event_sec = event.split('_')
        event = event_sec[0] + ''.join( [s.capitalize() for s in event_sec[1:]] )
        sertype = fstktype_for_modality[modality]
        fstk_name_guess = 'NDAR' + subj + '_' + event.replace('_','') + '_ABCD-%s_%s%s.tgz' % (sertype, series_date, series_time)


        # Update output dictionary with the resolved names of corresponding fast-track data files
        FasTrk_file = f_list[0]
        FasTrk_file_nopath = FasTrk_file[FasTrk_file.rfind('/')+1:]

        uFiles.update( dict( {'FasTrk_file':              FasTrk_file,
                              'FasTrk_file_nopath':       FasTrk_file_nopath,
                              'FasTrk_file_Guessed_Name': fstk_name_guess,
                              'series_date': series_date,
                              'series_time': series_time} ) )

        key = 'Run-%02.0f' % t_ord[j]
        FasTrk_files.update( dict( {key: uFiles} ) )

    return FasTrk_files
# ---------------------------------------------------------------------------------------------------------------
# ========================================================================================================================================================



# ========================================================================================================================================================
# ---------------------------------------------------------------------------------------------------------------
def Get_File_Names_and_Process_Info( subj, modality ):
    Files = {}

    #-------------------------------------------------------------------------------------------
    filoc = pd.read_csv( Dirs_Loc_fname, low_memory=False )
    filoc = filoc[ filoc['ProjID'] == 'DAL_ABCD' ]
    if modality == 'T1':
        scantype = 'MPR'
        fpath = filoc['proc'][0] + '/MRI'

    elif modality == 'T2':
        scantype = 'XetaT2'
        fpath = filoc['proc'][0] + '/MRI'

    elif modality == 'dMRI':
        scantype = 'DTI'
        fpath = filoc['proc_dti'][0] + '/DTI'

    elif 'fMRI' in modality:
        scantype = 'BOLD'
        fpath = filoc['proc_bold'][0] + '/BOLD'

    else:
        if Verbose:
            program_description()
            print('Error: Modality must be one of', modality_list )
        return {}
    #----------------------------------------------------------------------------------------

    #----------------------------------------------------------------------------------------
    Series, fdir, manuf  = Sers_from_ContainerInfo_and_PCinfo( subj, modality, scantype, fpath )
    
    scan_number = Series.index.tolist()

    if Verbose:
        print()
        print('Series:')
        print( Series )
        print()
        print('scan_number =', scan_number )
        print()
    #----------------------------------------------------------------------------------------

    #-------------------------------------------------------------------------------------------
    #                    Find names of result files and related fast-track files
    Proc_files = {}
    FasTrk_files = {}

    # if Verbose:
    #     print()
    #     print('Path_to_search, and found files:')

    if scantype in ['MPR', 'XetaT2']:

        Proc_files = T1T2_file_names_get( fdir, scantype, scan_number )

        if Verbose:
            print()
            print('Proc_files:')
            print( json.dumps( Proc_files, sort_keys=True, indent=2 ) )

        # Find corresponding /fast-track files according to series time
        FasTrk_files  =  FasTrk_files_names_get( Series, scan_number, subj, modality )


    elif scantype == 'BOLD':

        Proc_files  =  BOLD_file_names_get( fdir, scan_number, task_for_modality[modality], Series['t_ord'].tolist() )

        if Verbose:
            print()
            print('Proc_files:')
            print( json.dumps( Proc_files, sort_keys=True, indent=2 ) )

        # Find corresponding /fast-track files according to series date & time, and set additional variables
        FasTrk_files  =  FasTrk_files_names_get( Series, scan_number, subj, modality )


    elif scantype == 'DTI':

        Proc_files  =  DTI_file_names_and_RegMtx_get( fdir, manuf, scan_number )

        if Verbose:
            print()
            print('Proc_files:')
            print( json.dumps( Proc_files, sort_keys=True, indent=2 ) )

        # Find corresponding /fast-track files according to series date & time, and set additional variables
        FasTrk_files  =  FasTrk_files_names_get( Series, scan_number, subj, modality )


    if Verbose:
        print('FasTrk_files:')
        print( json.dumps( FasTrk_files, sort_keys=True, indent=2 ) )

    # Merge processed and fast-track file dictionaries:
    for k in Proc_files.keys():
        uRun = Proc_files[k]
        if k in FasTrk_files.keys():
            uRun.update( FasTrk_files[k] )
        else:
            Files.update( dict( {"FasTrk_file": "",  "FasTrk_file_nopath": ""} ) )
        Files.update( dict( {k: uRun} ) )
    #----------------------------------------------------------------------------------------

    #----------------------------------------------------------------------------------------
    if len(Files):
        # Incorporate additional variables to dictionary
        if modality == 'T1':
            var_list = addit_var_list + ['TI']
        else:
            var_list = addit_var_list

        for var in var_list:
            if var in Series.columns:
                for j,k in enumerate( Proc_files.keys() ):
                    uRun = Proc_files[k]
                    uRun.update( {var: Series[var][scan_number[j]]} )

                    # Phillips series do not have always TI in the normal place; if value is missing, get ito from ...private...
                    if var == 'TI':
                        if uRun['TI'] != uRun['TI']:  # value is nan
                            uRun['TI'] = Series['TIpriv'][scan_number[j]]

                    Files.update( dict( {k: uRun} ) )
    else:
        if Verbose:
            print('This process not to be shared; returning empty dictionary')
    #----------------------------------------------------------------------------------------

    if Verbose:
        print('\n- - - - - - - - - Files: - - - - - - - - -')
        print( json.dumps( Files, sort_keys=True, indent=2 ) )


    return Files
# ---------------------------------------------------------------------------------------------------------------
# ========================================================================================================================================================



# ========================================================================================================================================================
if __name__ == "__main__":

    subj, modality, Verbose  =  command_line_get_variables()

    Files  =  Get_File_Names_and_Process_Info( subj, modality )

    print( json.dumps( Files, sort_keys=True ) )
# ========================================================================================================================================================
