import atexit # module that allows you to register functions still when the script exits
from datetime import datetime
import logging
import os
from pathlib import Path
import pandas as pd
import sys
from zipfile import ZipFile, is_zipfile

SOURCE_FOLDER = '//OKBWPFILE02/ADPDataExtract$/TestHangingScript'

# create a logging file and formatting for it
logging.basicConfig(filename=f'{SOURCE_FOLDER}/app_log_{datetime.date(datetime.today())}.log', 
                        format='%(levelname)s: %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', 
                        encoding='utf-8', level=logging.DEBUG)

# Creating the initial dataframe to add to!
try:
    ADP_DF = pd.read_csv(SOURCE_FOLDER + '/master_candidates.csv', delimiter='|')
    logging.info('The csv file exists! Only new files will be added to it.')
except FileNotFoundError:
    logging.info('There is no csv file. Creating a new DataFrame object!')
    ADP_DF = pd.DataFrame()

# create a path object from pathlib module
adp_data = Path(SOURCE_FOLDER)


def export_master_csv(merged_df): 
    """
    Function that exports a master_csv file from the global ADP_DF df object
    """
    merged_df.sort_values(by=['candidate_id', 'folder_name'], inplace=True) # sort the df
    merged_df.to_csv(SOURCE_FOLDER + '/master_candidates.csv', chunksize=10000, sep='|', index=False) # export to csv file
    logging.info('Successfully exported master_candidates.csv')


def merge_csv(df):
    """
    Function that takes in a data frame and merges it
    """
    global ADP_DF
    if ADP_DF.empty: # check if the global ADP_DF dataframe is empty
        ADP_DF = df
        return 
    if not ADP_DF.empty: # check if it's not empty
        ADP_DF = pd.concat([ADP_DF, df])


def edit_create_csv(csv_files, folder):
    """
    Function that takes a path folder object iterates over it until it finds a candidates.csv and removes and creates a new csv file with the added folder name as a column
    """
    for csv in csv_files:
        if csv.name == 'candidates.csv':
            df = pd.read_csv(csv, delimiter='|', index_col=False)
            df['folder_name'] = folder.name
            # os.remove(csv) # remove the old csv file
            df.to_csv(csv, sep='|', index=False)
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
        if folder_path.stat().st_size > 0: # check if the folder size is greater than 0 (Basically checks if the folder already has extracted data...)
            logging.info(f'The folder {folder} already exists, No changes made!')
            return False
    return folder_path


def extract_update_files(filename):
    """
    Function that extracts zip files and updates them with the new column that indicates the folder it came from.
    """
    folder = filename.stem.replace(' ', '_') # replace the spaces with a underscore
    new_folder = create_folder(folder) # create new folder
    if new_folder != False:
        csv_files = []
        email_files = []
        with ZipFile(filename, 'r') as zip_ref:
            tracked_files = [file for file in zip_ref.namelist() if file.startswith('Attachments') and file.endswith('.msg') or file.endswith('.csv')]
            for file in tracked_files:
                if file.endswith('.csv'):
                    csv_files.append(new_folder.joinpath(file)) # joins the file string to the existing path object preserving the path object
                if file.endswith('.msg'):
                    email_files.append(new_folder.joinpath(file))
            try:
                zip_ref.extractall(path=new_folder)
            except PermissionError:
                logging.warning(f'Email file(s) are not permitted on the server.\nFLAGGED FILE - {email_files[0].name}')
            except WindowsError:
                logging.warning(f'Windows couldn\'t work with the {filename} for some reason. Nothing happened. See Error:')
            finally: edit_create_csv(csv_files, new_folder)


def main():
    
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        print('\nDependencies Installed!')
    else:
        print('running in a normal Python process')
    
    begin = datetime.now()
    print(f'\n\nScript started running at {begin}')
    
    for file in adp_data.iterdir():
        # check if the file is a zip file based off it's magic number from the zipFile module and futher check if it ends with .zip
        if is_zipfile(file) and str(file).endswith('.zip'):
            try:
                extract_update_files(file)
            except RuntimeError:
                logging.critical('program has terminated for some unexplained runtime reason!')
                atexit.register(export_master_csv, merged_df=ADP_DF) 
                sys.exit(1)
            except ConnectionError:
                logging.critical('program has terminated because there was an issue with the connection!')
                atexit.register(export_master_csv, merged_df=ADP_DF)  
                sys.exit(1) 

    end = datetime.now()
    execution_time = end - begin
    print(f'\nScript has finished running at {end}')
    logging.info(f'\nTotal execution time was {execution_time}')


if __name__ == "__main__":
    main()
    export_master_csv(ADP_DF) 
# to update executable run in the terminal : pyinstaller main.py