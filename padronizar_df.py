import pandas as pd

def organizar_colunas(dataframe):
    ordem_colunas = ['ean', 'produto', 'marca', 'farmacia', 'preco com desconto', 'preco sem desconto', '% desconto', 'descricao']
    return dataframe[ordem_colunas]

def padronizar_ordem_colunas(dataframe):
    df_padronizado = organizar_colunas(dataframe)
    return df_padronizado



