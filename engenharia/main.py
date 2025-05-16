
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

    agrupar(pasta)
    limpar(pasta)
    computar_atributos(pasta)
    salvar_sql(pasta)


if __name__ == "__main__":
    main()
