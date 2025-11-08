
from agrupar import agrupar
from limpar import limpar
from atributos import computar_atributos
from salvar_sql import salvar_sql

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def main():
    pasta = 'dados'
    nome_lote = '05-09-2025'

    agrupar(pasta, nome_lote)
    limpar(pasta)
    computar_atributos(pasta)
    salvar_sql(pasta)


if __name__ == "__main__":
    main()
