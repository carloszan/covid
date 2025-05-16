from tqdm import tqdm
import logging
import pandas as pd
from utils import registrar_execucao


def _suavizar(df, window_size=3, threshold=2):
    """
    Smooth data by replacing outliers with previous day's casosNovos.
    municipio, estado must be unique.

    Parameters:
    - df: pandas DataFrame with columns 'date' and 'casosNovos'
    - window_size: number of days to consider in rolling window
    - threshold: number of standard deviations to use for outlier detection

    Returns:
    - DataFrame with smoothed values in the column 'novos_casos_novos'
    """
    # Make a copy to avoid modifying original data
    df_smoothed = df.copy()

    df['novos_casos_novos'] = df['casosNovos']

    # Calculate rolling statistics
    rolling_mean = df['novos_casos_novos'].rolling(
        window=window_size, center=True, min_periods=1).mean()
    rolling_std = df['novos_casos_novos'].rolling(
        window=window_size, center=True, min_periods=1).std()

    # Identify outliers (values outside mean ± threshold*std)
    lower_bound = rolling_mean - threshold * rolling_std
    upper_bound = rolling_mean + threshold * rolling_std

    is_outlier = (df['novos_casos_novos'] < lower_bound) | (
        df['novos_casos_novos'] > upper_bound)

    # Replace outliers with previous day's value
    df_smoothed['novos_casos_novos'] = df['novos_casos_novos'].where(
        ~is_outlier, df['novos_casos_novos'].shift(1))
    df_smoothed['novos_casos_novos'] = df['novos_casos_novos'].where(
        ~is_outlier, df['novos_casos_novos'].shift(1))

    # For the first row (no previous value), use the next value if available
    if is_outlier.iloc[0] and len(df) > 1:
        df_smoothed.iloc[0, df_smoothed.columns.get_loc(
            'novos_casos_novos')] = df['novos_casos_novos'].iloc[1]

    return df_smoothed


def _recalcula_casos_acumulados(df):
    df['novos_casos_acumulados'] = df['casosNovos'].cumsum()
    return df


def _limpar(df):
    df = _suavizar(df)
    df = _recalcula_casos_acumulados(df)
    return df


def _processar(df):
    logging.info("Processando municipios_unicos")
    municipios_unicos = set(zip(df['municipio'], df['estado']))
    logging.info("Processamento concluído")

    logging.info("Filtrando o dataframe ")
    mascara = df.apply(lambda x: (
        x['municipio'], x['estado']) in municipios_unicos, axis=1)
    filtrado_df = df[mascara]
    logging.info("Processamento concluído")

    logging.info("Processando grupos")
    agrupado = filtrado_df.groupby(['municipio', 'estado'])
    logging.info("Processamento concluído")

    resultado_df = []

    for (municipio, estado), group_df in tqdm(agrupado, desc="Processando"):
        try:
            resultado_df.append(_limpar(group_df))
        except Exception as e:
            logging.error(f"{estado}_{municipio} não foi salvo")
            logging.error(e)

    resultado_df = pd.concat(resultado_df)
    return resultado_df


@registrar_execucao
def limpar(pasta):
    nome_arquivo = f'{pasta}/0.raw.parquet'

    logging.info(f"Lendo {nome_arquivo}")

    df = pd.read_parquet(f'{pasta}/0.raw.parquet')

    logging.info("Lido")

    processado_df = _processar(df)

    name_file = f'{pasta}/1.limpo.parquet'
    processado_df.to_parquet(name_file, index=False)

    logging.info(f"Parquet salvo com nome {name_file}")
