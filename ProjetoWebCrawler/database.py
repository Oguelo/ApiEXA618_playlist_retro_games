import sqlite3

NOME_BANCO = 'retro_games.db'

def obter_conexao():
    return sqlite3.connect(NOME_BANCO)

def inicializar_banco():
    conn = obter_conexao()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS jogos (
            url TEXT PRIMARY KEY,
            titulo TEXT,
            ano INTEGER,
            genero TEXT,
            plataforma TEXT,
            nota REAL,
            votos INTEGER,      
            imagem_url TEXT,
            link_download TEXT,
            data_extracao TEXT
        )
    ''')
    conn.commit()
    conn.close()

inicializar_banco()