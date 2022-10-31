import pandas as pd
import os
import glob
import sys
from datetime import datetime, date

#####################################################################################################
# call_dataframes finds the first and the second latest files that were called from the CONAGUA API,#
# then the files are concatenated, making a single file with the data from the last two hours,      #
# after that, the date from dloc is formated and filtered by the current date.                      #
#####################################################################################################
def call_dataframes(path):
    current_date = datetime.strptime(datetime.now().strftime('%Y-%m-%d'),'%Y-%m-%d').date()

    first_latest_file = sorted(glob.iglob('{}historic/wheather_data_*.csv'.format(path)), key=os.path.getctime)[-1]
    second_latest_file = sorted(glob.iglob('{}historic/wheather_data_*.csv'.format(path)), key=os.path.getctime)[-2]

    first_latest_df = pd.read_csv(first_latest_file)
    second_latest_df = pd.read_csv(second_latest_file)

    concat_dataframe = pd.concat([first_latest_df,second_latest_df])
    concat_dataframe['dloc'] =  pd.to_datetime(concat_dataframe['dloc'], format = '%Y-%m-%d')
    concat_dataframe['dloc'] = concat_dataframe['dloc'].dt.date
    
    current_date_df = concat_dataframe.loc[concat_dataframe['dloc'] == current_date]

    return current_date_df

#####################################################################################################
# get_averages takes the previous generated dataframe as an argument, then group by entity name,    #
# and select the columns idmun, ides, and find the mean from tmax and tmin, then return a new       #
# dataframe                                                                                         #
#####################################################################################################
def get_averages(dataframe):
    df_avg_temp = dataframe.groupby('nmun').agg({'idmun':'first','ides':'first','tmax':'mean','tmin':'mean'})
    return df_avg_temp

#####################################################################################################
# The path is passed as an argument in the shell terminal and is stored in a var called path,       #
# then the date is generated to be added to the name of the file that it's going to be generated    #
# then call_dataframes and get_averages are called, finally the average data is saved in a csv file #
#####################################################################################################
if __name__ == '__main__':
    path = sys.argv[1] # Pass the path as an argument in the execution of the script
    date = datetime.now().strftime('%y%m%d%H')
    two_hour_df = call_dataframes(path)
    avg_df = get_averages(two_hour_df)
    avg_df.to_csv('{}avg_data/two_hour_df_{}.csv'.format(path,date))

    
