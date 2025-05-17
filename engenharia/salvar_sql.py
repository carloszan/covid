import logging
from sqlalchemy import create_engine
import pandas as pd
from utils import registrar_execucao


@registrar_execucao
def salvar_sql(pasta):
    """
    Salva os dados processados em uma tabela de banco de dados PostgreSQL.

    Lê um arquivo Parquet contendo dados processados (com atributos) e os insere
    em uma tabela do PostgreSQL, substituindo os dados existentes.

    Args:
        pasta (str): Caminho da pasta base contendo o arquivo '2.atributos.parquet'
            que será carregado para o banco de dados.

    Returns:
        None: A função não retorna valores, mas realiza a inserção dos dados
        na tabela especificada do PostgreSQL.

    Example:
        >>> salvar_sql('dados/processados')
        # Lê 'dados/processados/2.atributos.parquet' e salva na tabela
        # 'new_new_covid' do PostgreSQL
    """

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
