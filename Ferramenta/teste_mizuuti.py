import copy
import json
import urllib
from datetime import datetime

import pandas as pd
import requests

from core.adaptees.SateliteImagery.cbers_imagery_service import CBERSImageryService

area = [-48.346435278654106, -16.117597211301963, -47.26538099348546, -15.432500881886043]
beginDate = datetime.datetime(2022, 8, 1)
endDate = datetime.datetime(2022, 8, 2)
folder = 'D:\\data'

cbers = CBERSImageryService()
files = cbers.download_images(area, beginDate, endDate, folder)
cbers.compose_image(files, folder)
