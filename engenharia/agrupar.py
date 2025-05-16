from pathlib import Path
import pandas as pd
import logging
from utils import registrar_execucao


@registrar_execucao
def agrupar(pasta):
    logging.info(f"Processo {__name__} iniciado")
    df = pd.DataFrame()

    directory_path = Path(f'{pasta}/raw')
    files = [f for f in directory_path.iterdir() if f.is_file()
             and f.name != ".gitkeep"]

    for file in files:
        logging.info(f"Lendo arquivo {file.name}")
        aux_df = pd.read_csv(f"{directory_path}/{file.name}", sep=';')
        df = pd.concat([df, aux_df])

    name_file = f'{pasta}/0.raw.parquet'
    df.to_parquet(name_file, index=False)

    logging.info(f"Parquet salvo com nome {name_file}")

    logging.info(f"Processo {__name__} finalizado")
