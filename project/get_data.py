import requests
import pandas as pd
import gzip
import datetime 
import sys  

#####################################################################################################
# get_data makes a request to the CONAGUA's API, then the file is saved in the gz folder, since the #
# file is downloaded as a gz compressed file,                                                       #
#####################################################################################################

def get_data(url,path,filename):
    response = requests.post(url)
    try:
        if response.status_code == 200:
            with open(r'{}gz/{}'.format(path,filename),'wb') as f:
                f.write(response.content)

    except:
        print('The service is unavailable at this time, please try later...')

#####################################################################################################
# format_data uncompress the downloaded file, the file with a JSON format is called and is flattened#
# and stored as a CSV file                                                                          #
#####################################################################################################

def format_data(path,in_fn,ou_fn):
    with gzip.open('{}gz/{}'.format(path,in_fn)) as f:
        weather_data = pd.read_json(f)
    weather_data.head()
    weather_data.to_csv("{}historic/{}".format(path,ou_fn), index = False)
    
#####################################################################################################
# The date is generated so we can use it as identifier of each downloaded file, the url is defined  #
# and the Project path is passed as an argument to the script, the output paths are formatted and   #
# get_data and format_data are called                                                               #
#####################################################################################################
if __name__ == '__main__':
    date = datetime.datetime.now().strftime('%y-%m-%d-%H-%M')
    #https://smn.conagua.gob.mx/webservices/?method=1
    url = 'https://smn.conagua.gob.mx/webservices/?method=1'
    #'C:\Users\Paola\Desktop\DE_assessment\'
    path = sys.argv[1] # Pass the path as an argument in the execution of the script
    gz_file = 'wheather_data_{}.gz'.format(date)
    output_file = 'wheather_data_{}.csv'.format(date)
    get_data(url,path, gz_file)    
    format_data(path,gz_file,output_file)
