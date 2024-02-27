import pandas as pd
import psycopg2
from psycopg2 import sql

# Baixando dataframes
# janeiro = pd.read_excel("SPDadosCriminais_2022-2022_1.xlsx")
# fevereiro = pd.read_excel("SPDadosCriminais_2022-2022_2.xlsx")
# marco = pd.read_excel("SPDadosCriminais_2022-2022_3.xlsx")
# abril = pd.read_excel("SPDadosCriminais_2022-2022_4.xlsx")
df = pd.read_excel("SPDadosCriminais_2022-2022_5.xlsx")
# junho = pd.read_excel("SPDadosCriminais_2022-2022_6.xlsx")
# julho = pd.read_excel("SPDadosCriminais_2022-2022_7.xlsx")
# agosto = pd.read_excel("SPDadosCriminais_2022-2022_8.xlsx")
# setembro = pd.read_excel("SPDadosCriminais_2022-2022_9.xlsx")
# outubro = pd.read_excel("SPDadosCriminais_2022-2022_10.xlsx")
# novembro = pd.read_excel("SPDadosCriminais_2022-2022_11.xlsx")
# dezembro = pd.read_excel("SPDadosCriminais_2022-2022_12.xlsx")

# Juntar informação dos dataframes
# df = pd.concat([janeiro, fevereiro, marco, abril, maio, junho, julho, agosto, setembro, outubro, novembro, dezembro])

# Corrigir dados com espaços em branco
df["CIDADE"] = df["CIDADE"].str.strip()

# Filtrando dados para o tipo de roubo e a região que iremos estudar
cidades = ['S.ANDRE', "S.BERNARDO DO CAMPO", 'S.CAETANO DO SUL']
natureza = ["FURTO - OUTROS", "FURTO DE CARGA", "FURTO DE VEÍCULO", "ROUBO - OUTROS", "ROUBO DE CARGA", "ROUBO DE VEÍCULO"]
df = df.loc[df['CIDADE'].isin(cidades)]
# df = df.loc[(df['CIDADE'] == "S.PAULO")]
df = df.loc[df['NATUREZA_APURADA'].isin(natureza)]

# Alterar nomes de tabelas para retirar acentos
df = df.rename(columns={'NOME_DELEGACIA_CIRCUNSCRIÇÃO': 'NOME_DELEGACIA_CIRCUNSCRICAO'})
df = df.rename(columns={'NOME_DEPARTAMENTO_CIRCUNSCRIÇÃO': 'NOME_DEPARTAMENTO_CIRCUNSCRICAO'})
df = df.rename(columns={'NOME_SECCIONAL_CIRCUNSCRIÇÃO': 'NOME_SECCIONAL_CIRCUNSCRICAO'})
df = df.rename(columns={'NOME_MUNICIPIO_CIRCUNSCRIÇÃO': 'NOME_MUNICIPIO_CIRCUNSCRICAO'})

# Conectando ao Banco de Dados
host = '******'
database = '*****'
user = '******'
password = '******'

conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password
)

# Criar a tabela no SQL
cur = conn.cursor()

column_types = {
    'NOME_DEPARTAMENTO': 'VARCHAR',
    'NOME_SECCIONAL': 'VARCHAR',
    'NOME_DELEGACIA': 'VARCHAR',
    'CIDADE': 'VARCHAR',
    'NUM_BO': 'VARCHAR',
    'ANO_BO': 'INTEGER',
    'DATA_COMUNICACAO_BO': 'DATE',
    'DATA_OCORRENCIA_BO': 'DATE',
    'HORA_OCORRENCIA_BO': 'TIME',
    'DESCR_PERIODO': 'VARCHAR',
    'DESCR_TIPOLOCAL': 'VARCHAR',
    'BAIRRO': 'VARCHAR',
    'LOGRADOURO': 'VARCHAR',
    'NUMERO_LOGRADOURO': 'VARCHAR',
    'LATITUDE': 'FLOAT',
    'LONGITUDE': 'FLOAT',
    'NOME_DELEGACIA_CIRCUNSCRICAO': 'VARCHAR',
    'NOME_DEPARTAMENTO_CIRCUNSCRICAO': 'VARCHAR',
    'NOME_SECCIONAL_CIRCUNSCRICAO': 'VARCHAR',
    'NOME_MUNICIPIO_CIRCUNSCRICAO': 'VARCHAR',
    'RUBRICA': 'VARCHAR',
    'DESCR_CONDUTA': 'VARCHAR',
    'NATUREZA_APURADA': 'VARCHAR',
    'MES_ESTATISTICA': 'INTEGER',
    'ANO_ESTATISTICA': 'INTEGER'
}

table_name = 'Dados_Criminais_ABC_2022'
"""columns = ", ".join([f"{col} {column_types[col]}" for col in df.columns])
create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
    sql.Identifier(table_name),
    sql.SQL(columns)
)

cur.execute(create_table_query)
conn.commit()"""

# Substituir Nan para Null
df.fillna(value=pd.NA, inplace=True)
df.replace(pd.NA, None, inplace=True)

# Inserir os dados do DataFrame na tabela
for index, row in df.iterrows():
    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({});").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(map(sql.Identifier, df.columns.str.lower())),
        sql.SQL(", ").join(map(sql.Literal, row))
    )
    cur.execute(insert_query)

conn.commit()

# Fechar conexões
cur.close()
conn.close()
