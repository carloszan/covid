import logging
from sqlalchemy import create_engine
import pandas as pd
from utils import registrar_execucao


@registrar_execucao
def salvar_sql(pasta):
    nome_arquivo = f'{pasta}/1.limpo.parquet'
    logging.info(f"Lendo {nome_arquivo}")

    df = pd.read_parquet(f'{pasta}/2.atributos.parquet')

    logging.info("Lido")

    engine = create_engine(
        'postgresql://root:dietpi@192.168.3.200:5432/new_covid')

    logging.info("Salvando no banco Postgres")

    df.to_sql('new_new_covid', con=engine, if_exists='replace',
              index=False, method='multi', chunksize=100000)

    logging.info('Salvo')
