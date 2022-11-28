# Script to scratch information from CSIC web page
from pathlib import Path
import yaml
from box import Box
import pandas as pd
from tqdm import tqdm
import logging
from typing import List, Union, Dict
logger = logging.getLogger(__name__)



def read_config():
    current_folder = Path().cwd()
    config_file = list(current_folder.glob('*config*.yaml'))[0]
    with open(config_file, 'r') as f:
        config = Box(yaml.safe_load(f), default_box=True)
    return config


def process_tables(tables: List[pd.DataFrame],
                   target_variables: List[str],
                   function_dict: Dict[str, callable],
                   id: str,
                   ):
    # info table
    table_dict = {}
    info_table = tables[0][0].dropna().reset_index(drop=True)
    for var in info_table.iloc[1:]:
        if target_variables == {}:
            if var in function_dict:
                table_dict[var] = function_dict[var](tables[info_table[info_table == var].index[0]], id)
            else:
                table_dict[var] = tables[info_table[info_table == var].index[0]]



def process_piezometry(df: pd.DataFrame, id: str) -> pd.DataFrame:
    pass

def process_chemical_analysis(df: pd.DataFrame, id: str) -> pd.DataFrame:
    pass

def process_lithologies(df: pd.DataFrame, id: str) -> pd.DataFrame:
    pass


function_dict = {
    'Medidas de piezometría': process_piezometry,
    'Análisis químicos': process_chemical_analysis,
    'Litologías': process_lithologies,
}





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
            print(process_tables(tables, target_variables=config.target_variables, function_dict=function_dict, id=id))
            for table_id, table in enumerate(tables):
                table.to_csv(cur_folder / f'{table_id}.csv')
            # break




