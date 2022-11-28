# Script to scratch information from CSIC web page
from pathlib import Path
import yaml
from box import Box
import pandas as pd
from tqdm import tqdm
import logging
logger = logging.getLogger(__name__)


def read_config():
    current_folder = Path().cwd()
    config_file = list(current_folder.glob('*config*.yaml'))[0]
    with open(config_file, 'r') as f:
        config = Box(yaml.safe_load(f))
    return config


if __name__ == '__main__':
    config = read_config()
    results_folder = Path('./results')
    results_folder.mkdir(exist_ok=True)
    for excel_file in config.excel_files:
        excel_results_file = results_folder / Path(excel_file).name
        excel_results_file.mkdir(exist_ok=True)
        id_list = pd.read_excel(excel_file, header=1)['Id']
        for id in tqdm(id_list, desc='processing urls'):
            print('Processing id: ', id)
            cur_folder = excel_results_file / str(id)
            cur_folder.mkdir(exist_ok=True)
            url = f'https://info.igme.es/BDAguasReport/Rpt/PointInfo.aspx?id={id}'
            tables = pd.read_html(url)
            for table_id, table in enumerate(tables):
                table.to_csv(cur_folder / f'{table_id}.csv')




