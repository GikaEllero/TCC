import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
pd.set_option('display.max_columns', None)

df = pd.read_excel("SPDadosCriminais_2022-2022_1.xlsx")

# Filtrando dados para o tipo de roubo e a região que iremos estudar
cidades = ['S.ANDRE', "S.BERNARDO DO CAMPO", 'S.CAETANO DO SUL']
natureza = ["FURTO - OUTROS", "FURTO DE CARGA", "FURTO DE VEÍCULO", "ROUBO - OUTROS", "ROUBO DE CARGA", "ROUBO DE VEÍCULO"]
df = df.loc[df['CIDADE'].isin(cidades)]
df = df.loc[(df['ANO_BO'] == 2022)]
df = df.loc[df['NATUREZA_APURADA'].isin(natureza)]

# Remover linhas nulas das colunas necessárias
df.dropna(subset=['DATA_OCORRENCIA_BO', 'DATA_COMUNICACAO_BO'], inplace=True)

# Modificar o tipo de dado considerado no dataframe
df["DATA_OCORRENCIA_BO"] = pd.to_datetime(df["DATA_OCORRENCIA_BO"])
df["NUM_BO"] = df["NUM_BO"].astype("int64")
df["DATA_OCORRENCIA_BO"] = pd.to_datetime(df["DATA_OCORRENCIA_BO"], format='%H:%M:%S')

print(df.head())
