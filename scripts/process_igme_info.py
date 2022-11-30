# Script to scratch information from CSIC web page
from pathlib import Path
import yaml
from box import Box
import pandas as pd
import tqdm
import logging
from typing import List, Union, Dict

# Create basic logger

class ColoredFormatter(logging.Formatter):
    """Special custom formatter for colorizing log messages!"""

    BLACK = '\033[0;30m'
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    BROWN = '\033[0;33m'
    BLUE = '\033[0;34m'
    PURPLE = '\033[0;35m'
    CYAN = '\033[0;36m'
    GREY = '\033[0;37m'

    DARK_GREY = '\033[1;30m'
    LIGHT_RED = '\033[1;31m'
    LIGHT_GREEN = '\033[1;32m'
    YELLOW = '\033[1;33m'
    LIGHT_BLUE = '\033[1;34m'
    LIGHT_PURPLE = '\033[1;35m'
    LIGHT_CYAN = '\033[1;36m'
    WHITE = '\033[1;37m'

    RESET = "\033[0m"

    def __init__(self, *args, **kwargs):
        self._colors = {logging.DEBUG: self.DARK_GREY,
                        logging.INFO: self.GREEN,
                        logging.WARNING: self.BROWN,
                        logging.ERROR: self.RED,
                        logging.CRITICAL: self.LIGHT_RED}
        super(ColoredFormatter, self).__init__(*args, **kwargs)

    def format(self, record):
        """Applies the color formats"""
        record.msg = self._colors[record.levelno] + record.msg + self.RESET
        return logging.Formatter.format(self, record)

    def setLevelColor(self, logging_level, escaped_ansi_code):
        self._colors[logging_level] = escaped_ansi_code


# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('test')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(ColoredFormatter())
logger.addHandler(ch)


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
        if var == 'Información del punto':
            continue
        elif 'System' in var:
            continue
        # if var in target_variables:
        logger.info(f'Processing variable {var} from id {id}')
        table_dict[var] = function_dict[var](tables[info_table[info_table == var].index[0]], id)
        # elif var in function_dict.keys():
        #     logger.warning(f'Detected variable {var} from id {id} but not processed')
        # else:
        #     logger.warning(f'{var} not implemented')
    return table_dict

def process_piezometry(df: pd.DataFrame, id: str) -> pd.DataFrame:
    # Drop first row
    df = df.drop(0).reset_index(drop=True)
    df.columns = df.iloc[0]
    df = df.drop(0).reset_index(drop=True)
    df['id'] = id
    return df



def process_lithologies(df: pd.DataFrame, id: str) -> pd.DataFrame:
    df = df.dropna().reset_index(drop=True)
    df.columns = df.iloc[0]
    # Drop first row
    df = df.drop(0).reset_index(drop=True)
    df['id'] = id
    return df

def process_chemical_analysis(df: pd.DataFrame, id: str) -> pd.DataFrame:
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


def process_igme_info():
    config = read_config()
    results_folder = Path('./results')
    results_folder.mkdir(exist_ok=True)
    assert config.target_variables is not None, 'target_variables must be defined in config file'
    for excel_file in config.excel_files:
        excel_results_file = results_folder / Path(excel_file).stem
        excel_results_file.mkdir(exist_ok=True)
        id_list = pd.read_excel(excel_file, header=1)['Id']
        data_dict = {}
        for id in tqdm.tqdm(id_list, desc='processing urls'):
            logger.info(f'\nProcessing id {id}')
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





