# Como carregar os dados

1. Baixar os dados no botão `Arquivo CSV` no [site](https://covid.saude.gov.br)
2. Extrair o arquivo na pasta /extraido **ou** utilizar o script `engenharia/1.extrair-dados.ipynb`
3. As analises podem utilizar a pasta /extraido em outros lugares. Fique atento para mover a pasta para perto das análises.


# Recarregar banco Postgres

1. Rodar o script `engenharia/2.carregar-dados.pg.ipynb`