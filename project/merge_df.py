import pandas as pd
import datetime
import glob
import os
import sys

def call_dataframes(path):
    file_name = sorted(glob.iglob('{}avg_data/*.csv'.format(path)),\
        key=os.path.getctime)[-1]
    two_hour_df = pd.read_csv('{}'.format(file_name))
    mun_file = sorted(glob.iglob('{}mun_data/*.csv'.format(path)),\
        key=os.path.getctime)[-1]
    mun_df = pd.read_csv('{}'.format(mun_file))

    return two_hour_df, mun_df

def merge_datasets(thd,md):
    new_df = pd.merge(md, \
                thd, \
                how = 'inner', \
                left_on=['Cve_Ent','Cve_Mun'], \
                right_on = ['ides','idmun']) \
                [['Cve_Ent','Cve_Mun','nmun','tmax','tmin','Value']]
    return new_df

if __name__ == '__main__':
    path = sys.argv[1] # Pass the path as an argument in the execution of the script
    current_path = '{}current/'.format(path)
    print(current_path)
    date = datetime.datetime.now().strftime('%y%m%d%H')
    thd, md = call_dataframes(path)
    nd = merge_datasets(thd,md)
    nd.to_csv('{}merged_dataframe_{}.csv'.format(current_path,date))



#C:\Users\Paola\Desktop\DE_assessment\
#'two_hour_df_22102923.csv'