import requests
from bs4 import BeautifulSoup
import time
import random
import concurrent.futures
from database import obter_conexao
from datetime import datetime



def buscar_links_generico(categoria, anoAtivo, limite_paginas):

    headers = {'User-Agent': 'Bot-Academico-EXA618/1.0'}
    links_coletados = []
    pagina_atual = 1
    
    while pagina_atual <= limite_paginas:
        if pagina_atual == 1:
            url_busca = f'https://www.myabandonware.com/browse/{categoria}/{anoAtivo}/'
        else:
            url_busca = f'https://www.myabandonware.com/browse/{categoria}/{anoAtivo}/page/{pagina_atual}/'
            
        print(f"  -> Lendo: {url_busca}")
        
        try:
            response = requests.get(url_busca, headers=headers)
            if response.status_code == 404:
                break
                
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            links_jogos = soup.find_all('a', class_='name')
            
            if not links_jogos:
                break
                
            for link in links_jogos:
                if 'href' in link.attrs:
                    url_completa = f"https://www.myabandonware.com{link['href']}"
                    if url_completa not in links_coletados:
                        links_coletados.append(url_completa)
            
            pagina_atual += 1
            time.sleep(1.5)
            
        except Exception as e:
            print(f"Erro ao buscar listagem na página {pagina_atual}: {e}")
            break
            
    return links_coletados


def extrair_dados_jogo(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    
    max_tentativas = 3
    
    for tentativa in range(max_tentativas):
        time.sleep(random.uniform(1.0, 3.0))
        
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 503:
                print(f"  [!] Bloqueio 503 detectado. Pausando 10s para esfriar o IP... (Tentativa {tentativa+1}/{max_tentativas})")
                time.sleep(10)
                continue
                
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            dados = {
                'Título': 'Não encontrado',
                'Gênero Principal': 'Não encontrado',
                'Plataforma Original': 'Não encontrado',
                'User Rating': 0.0,
                'Votos': 0,
                'Imagem': 'Sem imagem',
                'Data Lançamento': 'Não encontrado',
                'Link Download': 'Não encontrado'
            }
            
            caixa_principal = soup.find('div', class_='box')
            if caixa_principal:
                titulo_box = caixa_principal.find('h2')
                if titulo_box:
                    dados['Título'] = titulo_box.text.strip()
            
            tabela_info = soup.find('table', class_='gameInfo')
            if tabela_info:
                linhas = tabela_info.find_all('tr')
                for linha in linhas:
                    cabecalho = linha.find('th', scope='row')
                    valor = linha.find('td')
                    if cabecalho and valor:
                        texto_cabecalho = cabecalho.text.strip()
                        if texto_cabecalho == 'Genre':
                            dados['Gênero Principal'] = valor.text.strip()
                        elif texto_cabecalho == 'Platform':
                            dados['Plataforma Original'] = valor.text.strip()
                        elif texto_cabecalho == 'Released':
                            dados['Data Lançamento'] = valor.text.strip()
            
            rating_box = soup.find('div', id='grRaB', class_='gameRated')
            if rating_box:
                spans = rating_box.find_all('span')
                if len(spans) >= 3:
                    try:
                        dados['User Rating'] = float(spans[0].text.strip())
                        dados['Votos'] = int(spans[2].text.strip())
                    except ValueError:
                        pass
                        
            imagem_meta = soup.find('meta', property='og:image')
            if imagem_meta and 'content' in imagem_meta.attrs:
                dados['Imagem'] = imagem_meta['content']
                
            botoes = soup.find_all('a')
            for botao in botoes:
                if 'href' in botao.attrs and '/download/' in botao['href']:
                    dados['Link Download'] = f"https://www.myabandonware.com{botao['href']}"
                    break
            
            return dados
            
        except Exception as e:
            print(f"Erro na extração de {url}: {e}")
            if tentativa == max_tentativas - 1:
                return None
            time.sleep(5)
            
    return None
   
def rodar_populador():
    
    anos_para_processar = list(range(1993, 2001))
    
    conexao = obter_conexao()
    cursor = conexao.cursor()

    cursor.execute("SELECT url FROM jogos")
    urls_ja_salvas = {linha[0] for linha in cursor.fetchall()}


    for ano in anos_para_processar:
        print(f"\nBuscando páginas de {ano}...")
        
    
        links_do_ano = buscar_links_generico(categoria='year', anoAtivo=str(ano), limite_paginas=130)
        
        links_novos = [link for link in links_do_ano if link not in urls_ja_salvas]
        print(f"Total de jogos novos a processar: {len(links_novos)}")

        if not links_novos:
            continue
            
        print("Iniciando downloads simultâneos...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            resultados = list(executor.map(extrair_dados_jogo, links_novos))

        
            data_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            for url, dados in zip(links_novos, resultados):
                if dados:
                    cursor.execute('''
                        INSERT OR REPLACE INTO jogos 
                        (url, titulo, ano, genero, plataforma, nota, votos, imagem_url, link_download, data_extracao)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        url, dados['Título'], ano, dados['Gênero Principal'], 
                        dados['Plataforma Original'], dados['User Rating'], 
                        dados['Votos'], dados['Imagem'], 
                        dados['Link Download'], data_atual 
                    ))
                    
                    urls_ja_salvas.add(url)

        conexao.commit()
        print(f" Ano {ano} salvo ")

    conexao.close()
    print("\n PROCESSO FINALIZADO!")

if __name__ == '__main__':
    rodar_populador()