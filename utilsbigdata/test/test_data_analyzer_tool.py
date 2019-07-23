from utilsbigdata.pipeline_utils import data_analyzer_tool
from pathlib import Path
import os

def test_pipelane():
    files_dir = Path('/usr/src/code/car_data/datos_waze_2019_06_27')
    extraction_date = '2019.06.27'
    github_user = os.getenv('GITHUB_USER')
    github_token = os.getenv('GITHUB_TOKEN')
    data_pipeline = data_analyzer_tool.waze_data_analyzer(files_dir, extraction_date, github_user, github_token)
    project = 'mapa congestion jgibson v2'
    freq = '15min'
    agg_type = 'daytype'
    iqr_distance = 1.5
    data_pipeline.run_basic_data_pipeline(project, freq, agg_type, iqr_distance)
    data_pipeline.make_network_features()
 
if __name__ == '__main__':
    test_pipelane()