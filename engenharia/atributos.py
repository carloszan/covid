import pandas as pd
import logging
from utils import registrar_execucao


def _remover_colunas(df):
    """
    Remove colunas específicas de uma cópia do DataFrame.

    Cria uma cópia do DataFrame original e remove as colunas 'Recuperadosnovos' e
    'emAcompanhamentoNovos' do novo DataFrame. A operação é realizada inplace
    na cópia, não afetando o DataFrame original.

    Args:
        df (pd.DataFrame): DataFrame original que será copiado para remoção das colunas.

    Returns:
        None: A função não retorna explicitamente, pois a operação drop é feita inplace
        na cópia. Observação: há um possível erro de implementação, pois o retorno seria None
        devido ao parâmetro inplace=True.

    Example:
        >>> novo_df = df.copy()
        >>> _remover_colunas(df)
        # Cria cópia e remove colunas da cópia (retornando None)
    """
    novo_df = df.copy()
    return novo_df.drop(columns=["Recuperadosnovos",
                                 "emAcompanhamentoNovos"])


def _obter_estacao(data):
    """
    Determina a estação do ano (hemisfério sul) para uma data específica.

    Calcula a estação do ano com base nas datas astronômicas oficiais:
    - Verão: 21/12 a 20/03
    - Outono: 21/03 a 20/06
    - Inverno: 21/06 a 20/09
    - Primavera: 21/09 a 20/12

    Args:
        data (pd.Timestamp): Data que será analisada para determinar a estação.

    Returns:
        str: Nome da estação do ano correspondente ('Verão', 'Outono', 'Inverno' ou 'Primavera').

    Example:
        >>> _obter_estacao(pd.Timestamp('2023-07-15'))
        'Inverno'
        >>> _obter_estacao(pd.Timestamp('2023-10-10'))
        'Primavera'
    """
    ano = data.year
    datas_estacoes = {
        "verao": pd.Timestamp(f"{ano}-12-21"),
        "outono": pd.Timestamp(f"{ano}-3-21"),
        "inverno": pd.Timestamp(f"{ano}-6-21"),
        "primavera": pd.Timestamp(f"{ano}-9-21"),
    }

    if data >= datas_estacoes["verao"] or data < datas_estacoes["outono"]:
        return "Verão"
    elif data >= datas_estacoes["outono"] and data < datas_estacoes["inverno"]:
        return "Outono"
    elif data >= datas_estacoes["inverno"] and data < datas_estacoes["primavera"]:
        return "Inverno"
    else:
        return "Primavera"


def _traduzir_dias_da_semana(ingles):
    """
    Traduz o nome de um dia da semana de inglês para português.

    Converte o nome do dia da semana em inglês para sua correspondente
    tradução em português, incluindo os dias úteis com o sufixo '-feira'.

    Args:
        ingles (str): Nome do dia da semana em inglês (com primeira letra maiúscula).
            Valores aceitos: 'Sunday', 'Monday', 'Tuesday', 'Wednesday',
            'Thursday', 'Friday', 'Saturday'.

    Returns:
        str: Nome do dia da semana traduzido para português.

    Example:
        >>> _traduzir_dias_da_semana('Monday')
        'Segunda-feira'
        >>> _traduzir_dias_da_semana('Saturday')
        'Sábado'
    """
    portugues = {
        "Sunday": "Domingo",
        "Monday": "Segunda-feira",
        "Tuesday": "Terça-feira",
        "Wednesday": "Quarta-feira",
        "Thursday": "Quinta-feira",
        "Friday": "Sexta-feira",
        "Saturday": "Sábado"
    }

    return portugues[ingles]


def _traduzir_mes(ingles):
    """
    Traduz o nome de um mês de inglês para português.

    Converte o nome do mês em inglês para sua correspondente
    tradução em português, seguindo as convenções de nomenclatura
    dos meses em português brasileiro.

    Args:
        ingles (str): Nome do mês em inglês (com primeira letra maiúscula).
            Valores aceitos: 'January', 'February', 'March', 'April',
            'May', 'June', 'July', 'August', 'September',
            'October', 'November', 'December'.

    Returns:
        str: Nome do mês traduzido para português.

    Example:
        >>> _traduzir_mes('March')
        'Março'
        >>> _traduzir_mes('August')
        'Agosto'
    """
    portuguese = {
        "January": "Janeiro",
        "February": "Fevereiro",
        "March": "Março",
        "April": "Abril",
        "May": "Maio",
        "June": "Junho",
        "July": "Julho",
        "August": "Agosto",
        "September": "Setembro",
        "October": "Outubro",
        "November": "Novembro",
        "December": "Dezembro"
    }

    return portuguese[ingles]


def _adicionar_feature_datas(df):
    """
    Adiciona colunas de informações temporais traduzidas a um DataFrame.

    Cria novas colunas com informações derivadas da data:
    - Nome do mês em inglês e português
    - Dia da semana em inglês e português
    - Estação do ano correspondente

    Args:
        df (pd.DataFrame): DataFrame contendo uma coluna 'data' do tipo datetime.

    Returns:
        pd.DataFrame: Uma cópia do DataFrame original com as novas colunas adicionadas:
            - mes: nome do mês em inglês
            - mes_traduzido: nome do mês em português
            - dia_semana: dia da semana em inglês
            - dia_semana_traduzido: dia da semana em português
            - estacao: estação do ano
            - ano: ano extraído da data
            - mes_numerico: número do mês (1-12)
            - dia_semana_numerico: número do dia da semana (0-6, onde 0 é segunda)

    Example:
        >>> df = pd.DataFrame({'data': pd.to_datetime(['2023-01-15', '2023-07-20'])})
        >>> df_completo = _adicionar_feature_datas(df)
        # Retorna DataFrame com colunas adicionais de informações temporais
    """
    novo_df = df.copy()

    novo_df["ano"] = novo_df.data.dt.year
    novo_df["mes"] = novo_df.data.dt.month_name()
    novo_df["mes_numerico"] = novo_df.data.dt.month
    novo_df["mes_traduzido"] = novo_df["mes"].apply(_traduzir_mes)
    novo_df["dia_semana"] = novo_df.data.dt.day_name()
    novo_df["dia_semana_traduzido"] = novo_df["dia_semana"].apply(
        _traduzir_dias_da_semana)
    novo_df["dia_semana_numerico"] = novo_df.data.dt.day_of_week
    novo_df["estacao"] = novo_df["data"].apply(_obter_estacao)

    return novo_df


def _limpar_nomes_de_colunas(df):
    """
    Padroniza os nomes das colunas de um DataFrame para um formato consistente.

    Renomeia as colunas do DataFrame substituindo os nomes originais por versões
    padronizadas em snake_case (todas minúsculas com underscores), seguindo
    convenções de nomenclatura consistentes para melhor legibilidade e processamento.

    Args:
        df (pd.DataFrame): DataFrame com as colunas a serem renomeadas.

    Returns:
        pd.DataFrame: Um novo DataFrame com os nomes das colunas padronizados.

    Example:
        >>> df_limpo = _limpar_nomes_de_colunas(df_original)
        # Retorna o DataFrame com colunas renomeadas (ex: 'casosNovos' -> 'casos_novos')
    """
    colunas = {
        'regiao': 'regiao',
        'estado': 'estado',
        'municipio': 'municipio',
        'coduf': 'coduf',
        'codmun': 'codmun',
        'codRegiaoSaude': 'cod_regiao_saude',
        'nomeRegiaoSaude': 'nome_regiao_saude',
        'data': 'data',
        'semanaEpi': 'semana_epi',
        'populacaoTCU2019': 'populacao_tcu_2019',
        'casosAcumulado': 'casos_acumulados',
        'casosNovos': 'casos_novos',
        'novos_casos_novos': 'novos_casos_novos',
        'obitosAcumulado': 'obitos_acumulados',
        'obitosNovos': 'obitos_novos',
        'Recuperadosnovos': 'recuperados_novos',
        'emAcompanhamentoNovos': 'em_acompanhamento_novos',
        'interior/metropolitana': 'interior_metropolitana',
        'novos_casos_acumulados': 'novos_casos_acumulados'
    }
    return df.rename(columns=colunas)


def _rodar_engenharia_de_atributos(df):
    """
    Executa o pipeline completo de engenharia de atributos em um DataFrame.

    Realiza sequencialmente as seguintes operações:
    1. Remove colunas desnecessárias
    2. Adiciona features temporais (datas, meses, estações)
    3. Padroniza os nomes das colunas

    Args:
        df (pd.DataFrame): DataFrame original a ser processado.

    Returns:
        pd.DataFrame: DataFrame processado com colunas removidas, features adicionadas
        e nomes padronizados.

    Example:
        >>> df_processado = _rodar_engenharia_de_atributos(df_original)
        # Retorna o DataFrame após todas as transformações
    """
    df = _remover_colunas(df)
    df = _adicionar_feature_datas(df)
    df = _limpar_nomes_de_colunas(df)

    return df


@registrar_execucao
def computar_atributos(pasta):
    """
    Processa e computa atributos adicionais em um DataFrame a partir de um arquivo Parquet.

    Realiza a leitura de um arquivo Parquet pré-processado, aplica transformações
    de engenharia de atributos através da função _rodar_engenharia_de_atributos
    e salva o resultado em um novo arquivo Parquet.

    Args:
        pasta (str): Caminho da pasta base contendo o arquivo '1.limpo.parquet'
            e onde será salvo o resultado ('2.atributos.parquet').

    Returns:
        None: A função não retorna valores, mas gera um novo arquivo Parquet
        com os atributos processados.

    Example:
        >>> computar_atributos('dados/processados')
        # Lê 'dados/processados/1.limpo.parquet', processa os atributos
        # e salva em 'dados/processados/2.atributos.parquet'
    """

    nome_arquivo = f'{pasta}/1.limpo.parquet'

    logging.info(f"Lendo arquivo {nome_arquivo}")

    df = pd.read_parquet(
        nome_arquivo).astype({'data': 'datetime64[ns]'})

    logging.info(f"Arquivo lido")

    logging.info("Processando engenharia de atributos")

    df = _rodar_engenharia_de_atributos(df)

    logging.info("Processado")

    logging.info("Salvando arquivo")

    nome_arquivo = f'{pasta}/2.atributos.parquet'
    df.to_parquet(
        nome_arquivo,
        engine='pyarrow',
        coerce_timestamps='ms',
        allow_truncated_timestamps=True,
        index=False
    )

    logging.info(f"Parquet salvo com nome {nome_arquivo}")
