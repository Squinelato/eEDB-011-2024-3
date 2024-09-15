import pandas as pd

# Caminho para os arquivos .parquet
file_1 = '/home/anapriss/trabalho_spark/Atividade 4/etl_pipeline/raw/data_out/claims.parquet'
file_2 = '/home/anapriss/trabalho_spark/Atividade 4/etl_pipeline/raw/data_out/employees.parquet'

# Carregar os arquivos .parquet em dataframes separados
df1 = pd.read_parquet(file_1)
df2 = pd.read_parquet(file_2)

# Normalizar as colunas de interesse (remover espaços, deixar tudo em minúsculas)
df1['Instituição financeira'] = df1['Instituição financeira'].str.strip().str.lower()
df2['Nome'] = df2['Nome'].str.strip().str.lower()

# Remover a string "(conglomerado)" de 'Instituição financeira' em df1
df1['Instituição financeira'] = df1['Instituição financeira'].str.replace(r"\(conglomerado\)", "", regex=True).str.strip()

# Encontrar as diferenças: valores em df1 que não estão em df2
diferencas_df1_df2 = df1[~df1['Instituição financeira'].isin(df2['Nome'])]

# Encontrar as diferenças: valores em df2 que não estão em df1
diferencas_df2_df1 = df2[~df2['Nome'].isin(df1['Instituição financeira'])]

# Exibir somente as colunas de interesse
colunas_interesse_df1 = ['Instituição financeira']  # Adicione outras colunas se necessário
colunas_interesse_df2 = ['Nome']  # Adicione outras colunas se necessário

print("Diferenças encontradas em 'Instituição financeira' do df1 que não estão em 'Nome' do df2:")
print(diferencas_df1_df2[colunas_interesse_df1])

print("\nDiferenças encontradas em 'Nome' do df2 que não estão em 'Instituição financeira' do df1:")
print(diferencas_df2_df1[colunas_interesse_df2])

# Exibir as quantidades de diferenças
print("\nQuantidade de diferenças encontradas:")
print(f"No df1 que não estão no df2: {len(diferencas_df1_df2)}")
print(f"No df2 que não estão no df1: {len(diferencas_df2_df1)}")


# Iterar sobre os 14 valores encontrados e editar os nomes
for index, row in diferencas_df2_df1.iterrows():
    print(f"Nome atual: {row['Nome']}")
    novo_nome = input("Digite o novo nome para esse valor (ou deixe em branco para manter o atual): ").strip()
    if novo_nome:
        diferencas_df2_df1.at[index, 'Nome'] = novo_nome

# Exibir as alterações feitas
print("\nNomes atualizados:")
print(diferencas_df2_df1[['Nome']])


#TODO: 
# Trusted -> 
# Employtees: Conversão dos tipos de dados
#