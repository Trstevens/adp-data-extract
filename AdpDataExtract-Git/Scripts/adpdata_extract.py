import logging
import os
from pathlib import Path
import pandas as pd
from zipfile import ZipFile, is_zipfile

SOURCE_FOLDER = '//OKBWPFILE02/ADPDataExtract$/11282023_LoggingTest'
TEST_PATH = 'c:/Users/tstevens/Documents'

# create a logging file
logging.basicConfig(filename=f'{SOURCE_FOLDER}/app_log.log', format='%(levelname)s: %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', encoding='utf-8', level=logging.DEBUG)

# Creating the initial dataframe to add to!
try:
    print('\nAttempting to read the master_candidates.csv file...')
    ADP_DF = pd.read_csv(SOURCE_FOLDER + '/master_candidates.csv', delimiter='|')
    logging.info('The csv file exists! Only new files will be added to it.')
except FileNotFoundError:
    logging.debug('There is no csv file. Creating a new DataFrame object!')
    ADP_DF = pd.DataFrame()

# create a path object from pathlib module
adp_data = Path(SOURCE_FOLDER)
test_path = Path(TEST_PATH)


def export_master_csv(merged_df): 
    merged_df.sort_values(by=['candidate_id', 'folder_name'], inplace=True) # sort the df
    merged_df.to_csv(SOURCE_FOLDER + '/master_candidates.csv', chunksize=10000, sep='|', index=False) # export to csv file
    logging.info('Successfully exported master_candidates.csv')


def merge_csv(df):
    """Function that takes in a data frame and merges it"""
    global ADP_DF
    if ADP_DF.empty: # check if the global ADP_DF dataframe is empty
        ADP_DF = df
        return 
    if not ADP_DF.empty: # check if it's not empty
        ADP_DF = pd.concat([ADP_DF, df])


def edit_create_csv(folder):
    """
    Function that takes a path folder object iterates over it until it finds a candidates.csv and removes and creates a new csv file with the added folder name as a column
    """
    # child is every file within the folder
    for child in folder.iterdir():
        if child.name == 'candidates.csv':
            filepath = child # get the full file path to the candidates.csv file -- i.e.: //OKBWPFILE02/ADPDataExtract$/RMExport_Addus_HomeCare0/candidates.csv
            df = pd.read_csv(filepath, delimiter='|', index_col=False)
            df['folder_name'] = folder.name
            os.remove(child) # remove the old csv file
            df.to_csv(filepath, sep='|', index=False)
            merge_csv(df)
            # break out of the loop once the candidates.csv file is found
            break

def create_folder(folder):
    """
    Function that creates a folder from the path object
    """
    folder_path = Path(SOURCE_FOLDER + '/' + folder)
    try:
        folder_path.mkdir() # make the new directory from the folder path
        logging.info(f'Successfully made the folder: {folder}')
    except FileExistsError:
        logging.debug(f'The folder {folder} already exists, No changes made!')
        if folder_path.stat().st_size > 0: # check if the folder size is greater than 0 (Basically checks if the folder already has extracted data...)
            return False
    
    return folder_path


def extract_update_files(filename):
    """
    Function that extracts zip files and updates them with the new column that indicates the folder it came from.
    """
    with ZipFile(filename, 'r') as zip_ref:
        folder = filename.stem.replace(' ', '_') # replace the spaces with a underscore
        new_folder = create_folder(folder)
        if new_folder != False:
            try:
                zip_ref.extractall(path=new_folder)
                edit_create_csv(new_folder)
            except PermissionError:
                logging.warning()
            except WindowsError:
                logging.warning('Windows couldn\'t work with the file for some reason. Nothing happened. See Error:')


def main():
    print('\nStarting Script:')
    for file in adp_data.iterdir():
        # check if the file is a zip file based off it's magic number from the zipFile module and futher check if it ends with .zip
        if is_zipfile(file) and str(file).endswith('.zip'):
            extract_update_files(file)

    export_master_csv(ADP_DF)
    print('\nScript has finished running.')


main()