# import copy
# import json
# import os
# import urllib
# from datetime import datetime
#
# import arcpy
# import pandas as pd
# import requests
from datetime import datetime

# from core.adaptees.SateliteImagery.cbers_imagery_service import CBERSImageryService
#
# # area = [-48.346435278654106, -16.117597211301963, -47.26538099348546, -15.432500881886043]
# area = [-48.1959117, -15.5033938, -47.3136375, -16.0460539]
# beginDate = datetime.datetime(2022, 7, 1)
# endDate = datetime.datetime(2022, 7, 30)
# folder = 'D:\\data'
# b = 1
#
#
# cbers = CBERSImageryService()
# files = cbers.download_images(area, beginDate, endDate, folder)
# cbers.compose_image(files, folder)
# cbers.create_mosaic(folder)

import pandas as pd
import sqlalchemy

# data = []
# for i in range(3000000):
#     data.append({
#         'index': i,
#         'col1': f'{i}data{i}',
#         'col2': f'{i}data{i * 2}',
#         'col3': f'{i}data{i * 3}',
#         'col4': f'{i}data{i * 4}',
#         'col5': f'{i}data{i * 5}',
#         'col6': f'{i}data{i * 6}',
#         'col7': f'{i}data{i * 7}',
#         'col8': f'{i}data{i * 8}',
#         'col9': f'{i}data{i * 9}',
#         'col10': f'{i}data{i * 10}',
#         'col11': f'{i}data{i * 11}',
#         'col12': f'{i}data{i * 12}',
#         'col13': f'{i}data{i * 13}',
#         'col14': f'{i}data{i * 14}',
#         'col15': f'{i}data{i * 15}',
#         'col16': f'{i}data{i * 16}'
#     })
# df = pd.DataFrame(data)
# df.to_csv('records.csv')

df = pd.read_csv('records.csv')

dt1 = datetime.now()
engine = sqlalchemy.create_engine('postgresql://rmizuuti:mzd8sxp9@127.0.0.1:5432/teste')
#df.to_sql('img', engine, if_exists='append') #sem otimização
#df.to_sql('img', engine, chunksize=50000, if_exists='append') #chunksize1
# df.to_sql('img', engine, chunksize=10000, method='multi', if_exists='append') #multi
df.to_sql('img', engine, chunksize=1000, method='multi', if_exists='append') #chunksize2
dt2 = datetime.now()
print((dt2 - dt1).total_seconds())

#Sem orimização: 131.97s
#chunksize 50k: 121.29
#chunksize 10k: 117.71
#chunksize 5k: 115.29
#chunksize 1k: 113.90
#chunksize 50k & multi: 773.21