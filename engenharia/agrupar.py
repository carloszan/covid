from pathlib import Path
import pandas as pd
import logging
from utils import registrar_execucao


@registrar_execucao
def agrupar(pasta):
    """
    Agrupa arquivos CSV de um diretório em um único arquivo Parquet.

    Lê todos os arquivos CSV (excluindo '.gitkeep') de um diretório específico,
    combina-os em um único DataFrame e salva o resultado em um arquivo Parquet.

    Args:
        pasta (str): Caminho da pasta base onde os arquivos estão localizados.
            Espera-se que os arquivos CSV estejam em uma subpasta 'raw' dentro desta pasta.

    Returns:
        None: A função não retorna valores, mas salva um arquivo Parquet no diretório especificado.

    Example:
        >>> agrupar('dados/entrada')
        # Isso irá ler todos os CSVs em 'dados/entrada/raw/',
        # combinar em um DataFrame e salvar como 'dados/entrada/0.raw.parquet'
    """

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
