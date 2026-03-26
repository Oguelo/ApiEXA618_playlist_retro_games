import requests

def verificar_permissao(url_base):
    robots_url = f"{url_base.rstrip('/')}/robots.txt"
    try:
        res = requests.get(robots_url, timeout=10)
        if res.status_code == 200:
            print(f"--- Conteúdo do robots.txt de {url_base} ---")
            print(res.text)
        else:
            print(f"O site {url_base} não possui um robots.txt público (Status: {res.status_code}).")
    except Exception as e:
        print(f"Erro ao acessar: {e}")

verificar_permissao("https://www.myabandonware.com/")