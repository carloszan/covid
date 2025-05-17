from tqdm import tqdm
import logging
import pandas as pd
from utils import registrar_execucao


def _suavizar(df, window_size=3, threshold=2):
    """
    Aplica suavização estatística para remover outliers em séries temporais de casos novos.

    Utiliza uma abordagem de média móvel para identificar e corrigir valores atípicos:
    1. Calcula média e desvio padrão em janela móvel
    2. Identifica outliers como valores fora do intervalo (média ± threshold*desvio padrão)
    3. Substitui outliers pelo valor do dia anterior (ou próximo, para o primeiro registro)

    Args:
        df (pd.DataFrame): DataFrame contendo a coluna 'casosNovos' com os dados originais
        window_size (int, optional): Tamanho da janela para cálculo da média móvel. Default=3
        threshold (int, optional): Número de desvios padrão para definir outliers. Default=2

    Returns:
        pd.DataFrame: DataFrame com nova coluna 'novos_casos_novos' contendo os valores suavizados

    Example:
        >>> df_suavizado = _suavizar(df_original)
        >>> df_suavizado = _suavizar(df_original, window_size=5, threshold=3)
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
    new_df['novos_casos_acumulados'] = new_df['casosNovos'].cumsum()
    return new_df


def _limpar(df):
    """
    Executa o pipeline completo de limpeza e processamento dos dados.

    Realiza sequencialmente as seguintes operações:
    1. Suaviza os dados para remoção de outliers
    2. Recalcula os casos acumulados com base nos dados suavizados

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

    processado_df = _processar(df)

    name_file = f'{pasta}/1.limpo.parquet'
    processado_df.to_parquet(name_file, index=False)

    logging.info(f"Parquet salvo com nome {name_file}")
