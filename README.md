# Projeto

Esse código é a parte técnica do meu projeto de mestrado, com o título: `Um arcabouço computacional para monitoramento epidemiológico: caso de estudo Covid-19`.

O texto se encontra na pasta `docs/dissertacao.pdf`.

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
    nome_lote = '05-09-2025' # ESCOLHA O NOME DO LOTE

    agrupar(pasta, nome_lote)
    limpar(pasta)
    computar_atributos(pasta)
    # salvar_sql(pasta) # COMENTE ESSA LINHA
```

- Troque o paramêtro nome_lote se caso for necessário.
  - `nome_lote` é o nome da pasta a ser processado, dentro de `engenharia/dados/raw/[nome_lote]`.

## Execução
```
python engenharia/main.py
```


## Saída

- Arquivos processados estarão na pasta: engenharia/dados/
- Resultado final: 2.atributos.parquet