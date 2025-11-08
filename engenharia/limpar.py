from tqdm import tqdm
import logging
import pandas as pd
from utils import registrar_execucao
from sklearn.preprocessing import StandardScaler


def _remover_por_zscore(df):
    scaler = StandardScaler()
    LIMITE_COMUM = 1

    df["z_score"] = scaler.fit_transform(df[['casosNovos']])
    df_sem_outliers = df[abs(df["z_score"]) < LIMITE_COMUM]

    return df_sem_outliers


def _suavizar(df, window_size=7, threshold=2):
    """
    Smooth data by rolling mean.
    Df must be indexed.

    Parameters:
    - df: pandas DataFrame with columns 'date' and 'casosNovos'
    - window_size: number of days to consider in rolling window
    - threshold: number of standard deviations to use for outlier detection

    Returns:
    - DataFrame with smoothed values
    """
    # Make a copy to avoid modifying original data
    df_smoothed = df.copy()

    # Calculate rolling statistics
    rolling_mean = df['casosNovos'].rolling(
        window=window_size, center=False, closed='left').mean()

    df_smoothed['novos_casos_novos'] = pd.concat(
        [df['casosNovos'][0:window_size], rolling_mean[window_size:]])

    return df_smoothed


def _recalcula_casos_acumulados(df):
    """
    Recalcula os casos acumulados com base nos casos novos diários.

    Calcula a soma cumulativa dos casos novos ('casosNovos') e armazena
    o resultado em uma nova coluna 'novos_casos_acumulados'.

    Args:
        df (pd.DataFrame): DataFrame contendo a coluna 'casosNovos' com os dados diários.

    Returns:
        pd.DataFrame: Cópia do DataFrame original com a nova coluna 'novos_casos_acumulados'
        contendo a soma acumulada dos casos.

    Example:
        >>> df_atualizado = _recalcula_casos_acumulados(df_original)
        # Adiciona coluna com total acumulado de casos
    """
    new_df = df.copy()
    new_df['novos_casos_acumulados'] = new_df['novos_casos_novos'].cumsum()
    return new_df


def _limpar(df):
    """
    Executa o pipeline completo de limpeza e processamento dos dados.

    Args:
        df (pd.DataFrame): DataFrame original a ser processado, deve conter a coluna 'casosNovos'.

    Returns:
        pd.DataFrame: DataFrame processado com:
        - Dados suavizados na coluna 'novos_casos_novos'
        - Casos acumulados recalculados na coluna 'novos_casos_acumulados'

    Example:
        >>> df_limpo = _limpar(df_original)
        # Retorna o DataFrame após suavização e cálculo de acumulados
    """
    df = _remover_por_zscore(df)
    df = _suavizar(df)
    df = _recalcula_casos_acumulados(df)
    return df


def _processar(df):
    """
    Processa e limpa os dados de COVID-19 agrupados por município e estado.

    Realiza as seguintes operações:
    1. Identifica municípios únicos (combinação município/estado)
    2. Filtra o DataFrame para garantir consistência nos dados
    3. Agrupa os dados por município e estado
    4. Aplica a função _limpar para cada grupo (suavização e cálculo de acumulados)
    5. Combina todos os grupos processados em um único DataFrame

    Args:
        df (pd.DataFrame): DataFrame contendo os dados de COVID-19 com colunas:
            - municipio: nome do município
            - estado: sigla do estado
            - casosNovos: casos novos diários

    Returns:
        pd.DataFrame: DataFrame consolidado com todos os municípios processados,
        contendo colunas suavizadas e recalculadas.

    Example:
        >>> df_processado = _processar(df_original)
        # Retorna DataFrame com dados limpos e processados por município/estado
    """
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
    """
    Realiza a limpeza e pré-processamento de dados contidos em um arquivo Parquet.

    Lê um arquivo Parquet bruto, aplica transformações de limpeza através da função
    _processar e salva o resultado em um novo arquivo Parquet pré-processado.

    Args:
        pasta (str): Caminho da pasta base contendo o arquivo '0.raw.parquet'
            e onde será salvo o resultado ('1.limpo.parquet').

    Returns:
        None: A função não retorna valores, mas gera um novo arquivo Parquet
        com os dados limpos e processados.

    Example:
        >>> limpar('dados/processados')
        # Lê 'dados/processados/0.raw.parquet', aplica limpeza
        # e salva em 'dados/processados/1.limpo.parquet'
    """

    nome_arquivo = f'{pasta}/0.raw.parquet'

    logging.info(f"Lendo {nome_arquivo}")

    df = pd.read_parquet(f'{pasta}/0.raw.parquet')

    logging.info("Lido")

    logging.info(f"TAMANHO ANTES DO DF: {len(df)}")

    processado_df = _processar(df)

    logging.info(f"TAMANHO DEPOIS DO DF: {len(processado_df)}")

    diferenca = len(df) - len(processado_df)
    logging.info(f"DIFERENÇA: {diferenca}")
    logging.info(f"PORCENTAGEM: {diferenca / len(df)}")

    name_file = f'{pasta}/1.limpo.parquet'
    processado_df.to_parquet(name_file, index=False)

    logging.info(f"Parquet salvo com nome {name_file}")
