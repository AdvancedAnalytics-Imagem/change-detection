import copy
import json
import os
import urllib
from datetime import datetime

import arcpy
import pandas as pd
import requests

from core.adaptees.SateliteImagery.cbers_imagery_service import CBERSImageryService

# area = [-48.346435278654106, -16.117597211301963, -47.26538099348546, -15.432500881886043]
area = [-48.1959117, -15.5033938, -47.3136375, -16.0460539]
beginDate = datetime.datetime(2022, 7, 1)
endDate = datetime.datetime(2022, 7, 30)
folder = 'D:\\data'
b = 1


cbers = CBERSImageryService()
files = cbers.download_images(area, beginDate, endDate, folder)
cbers.compose_image(files, folder)
cbers.create_mosaic(folder)