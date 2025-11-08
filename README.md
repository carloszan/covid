# Como carregar os dados

# Pré-requisitos
```
pip install -r requirements.txt
```

## Configuração

- Atualize a connection string do banco no arquivo engenharia/salvar_sql.py.
```
CONNECTION_STRING = 'postgresql://root:dietpi@192.168.3.200:5432/new_covid'
```

- Para executar sem salvar no banco, apenas comente a linha no arquivo engenharia/main.py:
```
def main():
    pasta = 'dados'
    nome_lote = '05-09-2025'

    agrupar(pasta, nome_lote)
    limpar(pasta)
    computar_atributos(pasta)
    # salvar_sql(pasta) # COMENTE ESSA LINHA
```

## Execução
```
python engenharia/main.py
```


## Saída

- Arquivos processados estarão na pasta: engenharia/dados/
- Resultado final: 2.atributos.parquet