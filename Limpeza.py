import pandas as pd
import psycopg2

# Configurações
pd.set_option('display.max_columns', None)

# Conectando ao Banco de Dados
host = '********'
database = '***********'
user = '*********'
password = '**********'

conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password
)

# Consultar informações do banco de dados
table_name = 'Dados_Criminais_ABC_2022'
sql_query = 'SELECT * FROM "{}";'.format(table_name)

df = pd.read_sql_query(sql_query, conn)

# Fechar conexão
conn.close()

print(df.isnull().sum())

# Filtrando informações relevantes
# df = df.loc[(df['ANO_BO'] == 2022)]
# df = df.loc[(df['LOGRADOURO'] != "VEDAÇÃO DA DIVULGAÇÃO DOS DADOS RELATIVOS")]

# Remover linhas nulas das colunas necessárias
# df.dropna(subset=['DATA_OCORRENCIA_BO', 'DATA_COMUNICACAO_BO'], inplace=True)

print(df.shape)
