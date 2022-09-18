# -*- coding: utf-8 -*-
#!/usr/bin/python
import datetime
from datetime import datetime

from arcpy import GetParameter, GetParameterAsText, GetParameterInfo

from core._logs import *
from core.adapters.SateliteImagery import ImageAcquisition
from core.configs.Configs import Configs
from core.instances.Database import Database
from core.instances.Images import Image
from core.instances.MosaicDataset import MosaicDataset
from core.libs.BaseProperties import BaseProperties


def load_arcgis_variables(variables_obj: Configs) -> Configs:
    variables_obj.days_period = 30
    variables_obj.max_date = datetime.now()

    if len(GetParameterInfo())>0:
        variables_obj.debug = False
        variables_obj.arcgis_execution = True
    else:
        variables_obj.arcgis_execution = False
        variables_obj.debug = True

    if variables_obj.arcgis_execution:
        #* Área de interesse para a aquisição de imagens
        target_area = GetParameterAsText(0)
        if target_area and variables_obj.target_area != target_area:
            variables_obj.target_area = target_area
        
        #* Data máxima da busca
        max_date = GetParameter(1)
        if max_date:
            variables_obj.max_date = max_date

        #* Intervalo de dias anteriores
        days_period = GetParameter(2)
        if days_period:
            variables_obj.days_period = days_period
            
        #* Cobertura de nuvens (Opcional - 20%)
        max_cloud_coverage = GetParameter(3)
        if max_cloud_coverage:
            variables_obj.max_cloud_coverage = max_cloud_coverage

        #* Local de armazenamento final da imagem mosaicada e clipada
        image_storage = GetParameterAsText(4)
        if image_storage:
            os.environ['IMAGE_STORAGE'] = image_storage

        #* Mosaic Dataset ao qual a imagem deve ser adicionada (Opcional)
        output_mosaic_dataset = GetParameterAsText(5)
        if output_mosaic_dataset:
            variables_obj.output_mosaic_dataset = MosaicDataset(path=output_mosaic_dataset)

        #* Armazenamento temporario (Opcional - IN_MEMORY)
        temp_dir = GetParameterAsText(6)
        if temp_dir:
            os.environ['TEMP_DIR'] = temp_dir
            os.environ['TEMP_DB'] = os.path.join(temp_dir, f'{os.path.basename(temp_dir)}.gdb')

        #* Pasta para downloads (Opcional - core/dowloads)
        download_storage = GetParameterAsText(7)
        if download_storage:
            os.environ['DOWNLOAD_STORAGE'] = download_storage

        #* Deletar arquivos temporarios (Opcional - False)
        delete_temp_files = GetParameter(8)
        aprint(delete_temp_files)
        if delete_temp_files:
            os.environ['DELETE_TEMP_FILES'] = 'True'

    return variables_obj.init_base_variables()

VARIABLES = load_arcgis_variables(variables_obj=Configs())
BASE_CONFIGS = BaseProperties()

class DownloadSateliteImages:
    def __init__(self, variables: Configs, configs: BaseProperties):
        self.variables = variables
        self.configs = configs

    def get_image(self):
        aprint(message='Iniciando Aquisição de Imagem', progress=True)
        #* Stating Sensor Service Adapter
        image_acquisition_adapter = ImageAcquisition(
            service=self.variables.sensor, # TODO Check sensor type/string
            credentials=self.variables.credentials,
        )
        #* Pulling single image
        image = image_acquisition_adapter.get_composed_images_for_aoi(
            area_of_interest=self.variables.target_area,
            results_output_location=self.configs.temp_db,
            max_cloud_coverage=self.variables.max_cloud_coverage,
            max_date=self.variables.max_date,
            days_period=self.variables.days_period,
            compose_as_single_image=self.variables.compose_as_single_image # Caso negativo, os tiles não serão mosaicados em uma única imagem, isto impossibilita o append em um Mosaic Dataset
        )

        if hasattr(self.variables, 'output_mosaic_dataset') and self.variables.output_mosaic_dataset:
            new_image = self.add_image_to_mosaic_dataset(
                image=image,
                location=self.configs.image_storage,
                mosaic_dataset=self.variables.output_mosaic_dataset
            )
            return new_image
        return image
        
    def add_image_to_mosaic_dataset(self, image: Image, location: Database, mosaic_dataset: MosaicDataset, satelite: str = 'Sentinel2'):
        output_name = f'{satelite}_{image.date_created.strftime("%Y%m%d")}'
        new_image = Image(
            path=image.copy_image(
                delete_source=False,
                destination=location,
                output_name=output_name
            ),
            stretch_image=False
        )
        if self.variables.output_mosaic_dataset:
            mosaic_dataset.add_images(images=new_image)
            mosaic_dataset.footprints_layer.update_rows(
                where_clause=f"Name='{output_name}'",
                fields=['DataImagem','DataProcessamento'],
                values=[image.date_created, image.date_processed]
            )
        return new_image

if __name__ == '__main__':
    
    aprint(
        message=f'''
            \n1. Arquivos temporários serão salvos em: {BASE_CONFIGS.temp_dir}
            \n2. GeoDatabase temporário: {BASE_CONFIGS.temp_db.full_path}
            \n3. Arquivos baixados serão salvos em: {BASE_CONFIGS.download_storage}
            \n4. Imagens finais serão salvas em: {BASE_CONFIGS.image_storage}
        '''
    )

    image = DownloadSateliteImages(variables=VARIABLES, configs=BASE_CONFIGS).get_image()
    aprint(f'''\n_ _______________________ _
               \nAquisição de imagens concluída com sucesso, imagem final armazenada em:
               \n{image.full_path}
    ''')
    
    BASE_CONFIGS.delete_temporary_content()
