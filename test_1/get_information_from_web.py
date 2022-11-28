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
        config = Box(yaml.safe_load(f), default_box=True, default_box_attr=None)
    return config


def process_tables(tables: List[pd.DataFrame],
                   target_variables: List[str],
                   function_dict: Dict[str, callable],
                   id: str,
                   ) -> Dict[str, pd.DataFrame]:
    # info table
    table_dict = {}
    info_table = tables[0][0].dropna().reset_index(drop=True)
    for var in info_table.iloc[1:]:
        if var in target_variables:
            table_dict[var] = function_dict[var](tables[info_table[info_table == var].index[0]], id)
        else:
            print(f'Variable {var} cannot be processed')
    return table_dict




def process_piezometry(df: pd.DataFrame, id: str) -> pd.DataFrame:
    # Drop first row
    df = df.drop(0).reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df.drop(0).reset_index(drop=True)
    df['id'] = id
    return df

def process_chemical_analysis(df: pd.DataFrame, id: str) -> pd.DataFrame:
    print(df)

def process_lithologies(df: pd.DataFrame, id: str) -> pd.DataFrame:
    df = df.dropna().reset_index(drop=True)
    df.columns = df.iloc[0]
    # Drop first row
    df = df.drop(0).reset_index(drop=True)
    df['id'] = id
    return df



function_dict = {
    'Medidas de piezometría': process_piezometry,
    'Análisis químicos': process_chemical_analysis,
    'Litologías': process_lithologies,
}


if __name__ == '__main__':
    config = read_config()
    results_folder = Path('./results')
    results_folder.mkdir(exist_ok=True)
    assert config.target_variables is not None, 'target_variables must be defined in config file'
    for excel_file in config.excel_files:
        excel_results_file = results_folder / Path(excel_file).name
        excel_results_file.mkdir(exist_ok=True)
        id_list = pd.read_excel(excel_file, header=1)['Id']
        data_dict = {}
        for id in tqdm(id_list, desc='processing urls'):
            print('Processing id: ', id)
            url = f'https://info.igme.es/BDAguasReport/Rpt/PointInfo.aspx?id={id}'
            tables = pd.read_html(url)
            processed_tables = process_tables(tables, target_variables=config.target_variables, function_dict=function_dict, id=id)
            data_dict[id] = processed_tables

        for key in config.target_variables:
            concat_list = []
            for id in data_dict:
                case = data_dict[id]
                if key in case:
                    concat_list.append(case[key])
            pd.concat(concat_list).to_csv(excel_results_file / f'{key}.csv', index=False)





