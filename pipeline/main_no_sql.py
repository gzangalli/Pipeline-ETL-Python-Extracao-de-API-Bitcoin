import requests
from tinydb import TinyDB
import time

def extrair_dados():
    url = "https://api.coinbase.com/v2/prices/spot"

    response = requests.get(url)

    return response.json()


def transformar_dados(dados):
    valor = dados["data"]["amount"]
    criptomoeda = dados["data"]["base"]
    cotacao = dados["data"]["currency"]
    dados_transformados = {
        "valor": valor,
        "criptomoeda": criptomoeda,
        "cotacao": cotacao
    }
    return dados_transformados


def load(dados_tratados):
    db = TinyDB('db_teste_btc.json')
    db.insert(dados_tratados)
    print("Dados salvos com sucesso!")


if __name__ == "__main__":
    while True:
        dados = extrair_dados()
        dados_transformados = transformar_dados(dados)
        load(dados_transformados)
        time.sleep(15)
