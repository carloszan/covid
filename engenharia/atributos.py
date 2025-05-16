import pandas as pd
import logging
from utils import registrar_execucao


def _remover_colunas(df):
    df.drop(columns=["Recuperadosnovos",
            "emAcompanhamentoNovos"], inplace=True)
    return df


def _obter_estacao(data):
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
    df["mes"] = df.data.dt.month_name()
    df["mes_traduzido"] = df["mes"].apply(_traduzir_mes)
    df["dia_semana"] = df.data.dt.day_name()
    df["dia_semana_traduzido"] = df["dia_semana"].apply(
        _traduzir_dias_da_semana)
    df["estacao"] = df["data"].apply(_obter_estacao)

    return df


def _limpar_nomes_de_colunas(df):
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
    df = _remover_colunas(df)
    df = _adicionar_feature_datas(df)
    df = _limpar_nomes_de_colunas(df)

    return df


@registrar_execucao
def computar_atributos(pasta):
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
