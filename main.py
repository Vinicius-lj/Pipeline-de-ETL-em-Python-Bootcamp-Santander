import requests
import pandas

# Pipeline de ETL para retornar a inflação acumulada desde o ano de nascimento de usuários.
# Extract
df = pandas.read_csv('Idade de Usuarios.csv')

anos_de_nascimento = {}

for index, row in df.iterrows():
    nome = row['Nome']
    ano = row['Ano de Nascimento']
    anos_de_nascimento[nome] = ano

# API key in https://www.alphavantage.co/support/#api-key
api_key = 'Sua API key'
url = f'https://www.alphavantage.co/query?function=INFLATION&apikey={api_key}'
response = requests.get(url)
dados = response.json()

# Tranform
data_fim = '2022-01-01'
rentabilidade = []
lista_inflacao_acumulada = []

for ano_user in anos_de_nascimento.values():
    inflacao_acumulada = 0
    for anos in dados['data']:
        data = anos['date']
        inflacao = float(anos['value'])
        if ano_user <= data <= data_fim:
            inflacao_acumulada += inflacao
    lista_inflacao_acumulada.append(round(inflacao_acumulada, 2))

# Load - Devolve um csv com a adição da coluna Inflação Acumulada
df["Inflação Acumulada"] = lista_inflacao_acumulada
df.to_csv("Idade de Usuarios com Inflação.csv", index=False)
