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

cbers = CBERSImageryService()
cbers.get_images_metadata(area, beginDate, endDate)


LYR_CBERS = "CBERS 4A"
LYR_GRID_CBERS = None

#DELTA_DAYS          = 210 #90 #Foi utilizado as datas diretamente para simplificar os testes
CLOUD_COVER         = 50
CLEAR_OUTPUT_IMAGES = 180 # DAYS

CBERS_FIELDS    = ['ID','A_DATE','A_CLOUDCOVER','H_DATE','H_CLOUDCOVER']
CBERS_URL       = 'http://www2.dgi.inpe.br/stac-compose/stac/search/'
CBERS_USER      = 'caesb_testes@outlook.com'
CBER_SENSOR     = 'WPM'
CBER_OUTPUT     = ''

beginDate = datetime(2022, 8, 1)
endDate = datetime(2022, 8, 2)
cloudCover = CLOUD_COVER

print(' # PARÂMETROS')
print(f'''   + COBERTURA DE NUVENS: {cloudCover}''')
print(f'''   + DATA INÍCIO:         {beginDate}''')
print(f'''   + DATA FIM:            {endDate}''')

CBERS_PAYLOAD = json.dumps({
  "providers": [
  {
    "name": "INPE-CDSR",
    "collections": [
      { "name": "CBERS4A_WPM_L4_DN" },
      { "name": "CBERS4A_WPM_L2_DN" },
      { "name": "CBERS4A_WFI_L4_DN" },
      { "name": "CBERS4A_WFI_L2_DN" },
      { "name": "CBERS4A_MUX_L4_DN" },
      { "name": "CBERS4A_MUX_L2_DN" }
    ],
    "method": "POST",
    "query": { "cloud_cover": { "lte": CLOUD_COVER } }
  }
],
"bbox": [ -48.346435278654106, -16.117597211301963, -47.26538099348546, -15.432500881886043 ],
"datetime": f"{datetime.strftime(beginDate, '%Y-%m-%d')}T00:00:00/{datetime.strftime(endDate, '%Y-%m-%d')}T23:59:00",
"limit": 10000
})
CBERS_HEADERS = { 'Content-Type': 'application/json' }
CBERS_FIELDS = ['id','path','row','datetime','cloudcover']

response = requests.request('POST', CBERS_URL, headers=CBERS_HEADERS, data=CBERS_PAYLOAD)

CBERS_SCENES = []
jsonScene = { 'id': '',
             'datetime': '',
             'cloudcover': 0,
             'path': 0,
             'row': 0,
             'pan_url': '',
             'blue_url': '',
             'green_url': '',
             'red_url': '',
             'nir_url': '' }

if(CBER_SENSOR == 'WPM'):
  # CBERS4A_WPM_L4_DN
  for scene in json.loads(response.text)['INPE-CDSR']['CBERS4A_WPM_L4_DN']['features']:
    sceneMetadata = copy.deepcopy(jsonScene)
    sceneMetadata['id'] = scene['id']
    sceneMetadata['datetime'] = scene['properties']['datetime']
    sceneMetadata['cloudcover'] = scene['properties']['cloud_cover']
    sceneMetadata['path'] = scene['properties']['path']
    sceneMetadata['row'] = scene['properties']['row']
    sceneMetadata['pan_url'] = f"{scene['assets']['pan']['href']}?email={CBERS_USER}"
    sceneMetadata['red_url'] = f"{scene['assets']['red']['href']}?email={CBERS_USER}"
    sceneMetadata['blue_url'] = f"{scene['assets']['blue']['href']}?email={CBERS_USER}"
    sceneMetadata['green_url'] = f"{scene['assets']['green']['href']}?email={CBERS_USER}"
    sceneMetadata['nir_url'] = f"{scene['assets']['nir']['href']}?email={CBERS_USER}"

    CBERS_SCENES.append(sceneMetadata)

  # CBERS4A_WPM_L2_DN
  for scene in json.loads(response.text)['INPE-CDSR']['CBERS4A_WPM_L2_DN']['features']:
    sceneMetadata = copy.deepcopy(jsonScene)
    sceneMetadata['id'] = scene['id']
    sceneMetadata['datetime'] = scene['properties']['datetime']
    sceneMetadata['cloudcover'] = scene['properties']['cloud_cover']
    sceneMetadata['path'] = scene['properties']['path']
    sceneMetadata['row'] = scene['properties']['row']
    sceneMetadata['pan_url'] = f"{scene['assets']['pan']['href']}?email={CBERS_USER}"
    sceneMetadata['red_url'] = f"{scene['assets']['red']['href']}?email={CBERS_USER}"
    sceneMetadata['blue_url'] = f"{scene['assets']['blue']['href']}?email={CBERS_USER}"
    sceneMetadata['green_url'] = f"{scene['assets']['green']['href']}?email={CBERS_USER}"
    sceneMetadata['nir_url'] = f"{scene['assets']['nir']['href']}?email={CBERS_USER}"

    CBERS_SCENES.append(sceneMetadata)

dfCBERS = pd.DataFrame(CBERS_SCENES).sort_values(by=['cloudcover','datetime'], ascending=[True, False])
tiles = []

DOWNLOAD_PATH = 'D:\\data'

for index, row in dfCBERS.iterrows():
    name = f"{DOWNLOAD_PATH}\\p_{row['id']}.tif"
    urllib.request.urlretrieve(row['pan_url'], name)
    name = f"{DOWNLOAD_PATH}\\red_{row['id']}.tif"
    urllib.request.urlretrieve(row['red_url'], f'{DOWNLOAD_PATH}\\{name}')
    name = f"{DOWNLOAD_PATH}\\green_{row['id']}.tif"
    urllib.request.urlretrieve(row['green_url'], f'{DOWNLOAD_PATH}\\{name}')
    name = f"{DOWNLOAD_PATH}\\blue_{row['id']}.tif"
    urllib.request.urlretrieve(row['blue_url'], f'{DOWNLOAD_PATH}\\{name}')
    name = f"{DOWNLOAD_PATH}\\nir_{row['id']}.tif"
    urllib.request.urlretrieve(row['nir_url'], f'{DOWNLOAD_PATH}\\{name}')
a = 1
