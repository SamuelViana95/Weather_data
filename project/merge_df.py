import pandas as pd
import datetime
import glob
import os
import sys

#####################################################################################################
# call_dataframes call the average table and the mun_data file, then a dataframe of each is created #
# and passed as variables to the next function                                                      #
#####################################################################################################

def call_dataframes(path):
    file_name = sorted(glob.iglob('{}avg_data/*.csv'.format(path)),\
        key=os.path.getctime)[-1]
    two_hour_df = pd.read_csv('{}'.format(file_name))
    mun_file = sorted(glob.iglob('{}mun_data/*.csv'.format(path)),\
        key=os.path.getctime)[-1]
    mun_df = pd.read_csv('{}'.format(mun_file))

    return two_hour_df, mun_df


#####################################################################################################
# merge_datasets takes both dataframes generated in the previous function, then both are inner      #
# joined by two keys, Entity key and Mun Key, then 6 columns are selected                           #
#####################################################################################################
def merge_datasets(thd,md):
    new_df = pd.merge(md, \
                thd, \
                how = 'inner', \
                left_on=['Cve_Ent','Cve_Mun'], \
                right_on = ['ides','idmun']) \
                [['Cve_Ent','Cve_Mun','nmun','tmax','tmin','Value']]
    return new_df

#####################################################################################################
# The project path is passed as an argument to the script, then the path to the Current folder is  ,#
# generated, the date is generated as an identifier for the merged file, call_dataframes and        #
# merge_datasets are called and the output is stored as a CSV file in the Current folder            #
#####################################################################################################
if __name__ == '__main__':
    path = sys.argv[1] # Pass the path as an argument in the execution of the script
    current_path = '{}current/'.format(path)
    print(current_path)
    date = datetime.datetime.now().strftime('%y%m%d%H')
    thd, md = call_dataframes(path)
    nd = merge_datasets(thd,md)
    nd.to_csv('{}merged_dataframe_{}.csv'.format(current_path,date))
