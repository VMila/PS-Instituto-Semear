import pandas as pd
import sqlite3
import seaborn as sns
import matplotlib.pyplot as plt

meses = {
    "Janeiro": "01",
    "Fevereiro": "02",
    "Março": "03",
    "Abril": "04",
    "Maio": "05",
    "Junho": "06",
    "Julho": "07",
    "Agosto": "08",
    "Setembro": "09",
    "Outubro": "10",
    "Novembro": "11",
    "Dezembro": "12",
}

trimestres = {
    "1T23": ("01", "02", "03"),
    "2T23": ("04", "05", "06"),
    "3T23": ("07", "08", "09"),
    "4T23": ("10", "11", "12"),
}

dataBase = pd.read_csv('Frame Case Analista de Dados - BASE.csv')
dataFollow = pd.read_csv('Frame Case Analista de Dados - FOLLOW.csv', header=1)

dataFollow.columns = ['Unnamed1', 'IMPACTADOS', 'Total_Impactados', 'Unnamed4', 'CH', 'Total_CH']


conn = sqlite3.connect(':memory:')

dataBase['CH'] = dataBase['CH'].str.replace(',', '.').astype(float)
dataBase.to_sql('tabelaBase', conn, index=False, if_exists='replace')
dataFollow.to_sql('tabelaFollow', conn, index=False, if_exists='replace')
cursor = conn.cursor()

query_change_date_format_base = """UPDATE tabelaBase
SET "Data Atividade" = substr("Data Atividade", 7, 4) || '-' || substr("Data Atividade", 4, 2) || '-' || substr("Data Atividade", 1, 2);
"""
cursor.execute(query_change_date_format_base)

query_change_date_format_follow = """UPDATE tabelaFollow
SET IMPACTADOS = substr(IMPACTADOS, 7, 4) || '-' || substr(IMPACTADOS, 4, 2) || '-' || substr(IMPACTADOS, 1, 2),
    CH = substr(CH, 7, 4) || '-' || substr(CH, 4, 2) || '-' || substr(CH, 1, 2)
WHERE substr(IMPACTADOS, 3, 1) = '/' OR substr(CH, 3, 1) = '/';
"""
cursor.execute(query_change_date_format_follow)

query_add_column = "ALTER TABLE tabelaBase ADD COLUMN ConferenciaCPF TEXT;"
cursor.execute(query_add_column)

query_add_column = "ALTER TABLE tabelaBase ADD COLUMN Impactado TEXT;"
cursor.execute(query_add_column)

query_create_table = """
CREATE TABLE tabelaBase_ordenada AS
SELECT *
FROM tabelaBase
ORDER BY CPF, "Atividade";
"""
cursor.execute(query_create_table)

query_create_table = """
CREATE TABLE tabelaFollow_drop AS
SELECT IMPACTADOS, Total_Impactados, CH, Total_CH
FROM tabelaFollow;
"""
cursor.execute(query_create_table)

query_update_column = "UPDATE tabelaBase_ordenada SET ConferenciaCPF = CPF;"
cursor.execute(query_update_column)

query_remove_chars = """
UPDATE tabelaBase_ordenada
SET ConferenciaCPF = REPLACE(REPLACE(ConferenciaCPF, '.', ''), '-', '');
"""
cursor.execute(query_remove_chars)

query_add_zeros = """
UPDATE tabelaBase_ordenada
SET ConferenciaCPF = printf('%011d', CAST(ConferenciaCPF AS INTEGER));
"""
cursor.execute(query_add_zeros)

query_update_column = """
UPDATE tabelaBase_ordenada
SET Impactado = CASE
    WHEN CH >= 1 THEN 1
    ELSE 0
END;
"""
cursor.execute(query_update_column)

query_cpfs_com_impactado = """
SELECT DISTINCT ConferenciaCPF
FROM tabelaBase_ordenada
WHERE Impactado = 1;
"""
cpfs_com_impactado = pd.read_sql_query(query_cpfs_com_impactado, conn)

for cpf in cpfs_com_impactado['ConferenciaCPF']:
    contador = 0
    query_linhas_cpf = f"SELECT rowid, * FROM tabelaBase_ordenada WHERE ConferenciaCPF = '{cpf}' ORDER BY rowid;"
    linhas_cpf = pd.read_sql_query(query_linhas_cpf, conn)
    for index, row in linhas_cpf.iterrows():
        if row['CH'] >= 1:
            contador += 1
            query_update_impactado = f"""
            UPDATE tabelaBase_ordenada
            SET Impactado = CASE
                WHEN CH >= 1 AND {contador} = 1 THEN 1
                ELSE 0
            END
            WHERE rowid = {row['rowid']};
            """
            cursor.execute(query_update_impactado)

query_zera = """
UPDATE tabelaFollow_drop
SET Total_Impactados = 0,
    Total_CH = 0;
"""
cursor.execute(query_zera)

query_incrementa_follow_metas = """
UPDATE tabelaFollow_drop
SET Total_Impactados = Total_Impactados + (
    SELECT COUNT(*)
    FROM tabelaBase_ordenada
    WHERE tabelaBase_ordenada."Data Atividade" BETWEEN tabelaFollow_drop.IMPACTADOS
                                                  AND date(tabelaFollow_drop.IMPACTADOS, '+6 days')
    AND tabelaBase_ordenada.Impactado = 1
),
    Total_CH = (
    SELECT COALESCE(SUM(CH), 0)
    FROM tabelaBase_ordenada
    WHERE tabelaBase_ordenada."Data Atividade" BETWEEN tabelaFollow_drop.CH
                                                  AND date(tabelaFollow_drop.CH, '+6 days')
);
"""
cursor.execute(query_incrementa_follow_metas)

query_update_totals = """
UPDATE tabelaFollow_drop
SET Total_Impactados = (
    SELECT COALESCE(SUM(Impactado), 0)
    FROM tabelaBase_ordenada
),
Total_CH = (
    SELECT COALESCE(SUM(CH), 0)
    FROM tabelaBase_ordenada
)
WHERE CH = "2024";
"""
cursor.execute(query_update_totals)

for mes, num_mes in meses.items():
    query_update_totals = f"""
    UPDATE tabelaFollow_drop
    SET Total_Impactados = (
        SELECT COALESCE(SUM(Total_Impactados), 0)
        FROM tabelaFollow_drop
        WHERE substr("IMPACTADOS", 6, 2) = '{num_mes}'
    ),
    Total_CH = (
        SELECT COALESCE(SUM(Total_CH), 0)
        FROM tabelaFollow_drop
        WHERE substr("IMPACTADOS", 6, 2) = '{num_mes}'
    )
    WHERE CH = '{mes}';
    """
    cursor.execute(query_update_totals)

for trimestre, meses in trimestres.items():
    meses_sql = "', '".join(meses) 
    
    query_update_totals = f"""
    UPDATE tabelaFollow_drop
    SET Total_Impactados = Total_Impactados + (
        SELECT COALESCE(SUM(Total_Impactados), 0)
        FROM tabelaFollow_drop
        WHERE substr("IMPACTADOS", 6, 2) IN ('{meses_sql}')
    ),
    Total_CH = Total_CH + (
        SELECT COALESCE(SUM(Total_CH), 0)
        FROM tabelaFollow_drop
        WHERE substr("IMPACTADOS", 6, 2) IN ('{meses_sql}')
    )
    WHERE CH = '{trimestre}';
    """
    cursor.execute(query_update_totals)


query_verifica = """SELECT * FROM tabelaFollow_drop;"""
resultado = pd.read_sql_query(query_verifica, conn)
resultado .to_csv('tabela_follow.csv', index=False)

query_verifica = """SELECT * FROM tabelaBase_ordenada;"""
resultado = pd.read_sql_query(query_verifica, conn)
resultado .to_csv('tabela_base.csv', index=False)
print(resultado)

dataBase_atualizado = pd.read_csv('tabela_base.csv')
correlacao = dataBase_atualizado[['CH', 'Impactado']].corr()
sns.scatterplot(x=dataBase_atualizado['CH'], y=dataBase_atualizado['Impactado'])
plt.title(f'Correlação entre CH e Impactado: {correlacao.loc["CH", "Impactado"]:.2f}')
plt.show()

