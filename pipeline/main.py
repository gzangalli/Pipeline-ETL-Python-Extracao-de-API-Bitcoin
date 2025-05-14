import requests
import time
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv('../')

DATABASE_URL = os.getenv("DATABASE_KEY")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()


class BitcoinDados(Base):
    __tablename__ = "bitcoin_dados"
    id = Column(Integer, primary_key=True)
    valor = Column(Float)
    criptomoeda = Column(String(10))
    cotacao = Column(String(10))
    timestamp = Column(DateTime)

    def __repr__(self):
        return (f"<BitcoinDados(valor={self.valor}, criptomoeda='{self.criptomoeda}', "
                f"cotacao='{self.cotacao}', timestamp='{self.timestamp}')>")


# Cria a tabela (se não existir)
Base.metadata.create_all(engine)
# Base.metadata.drop_all(engine)


def extrair_dados_bitcoin():
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    return response.json()


""" 
def transformar_dados(dados):
    valor = float(dados["data"]["amount"])
    criptomoeda = dados["data"]["base"]
    cotacao = dados["data"]["currency"]
    dados_transformados = {
        "valor": valor,
        "criptomoeda": criptomoeda,
        "cotacao": cotacao
    }
    return dados_transformados
 """


def transformar_dados_bitcoin(dados):
    valor = float(dados["data"]["amount"])
    criptomoeda = dados["data"]["base"]
    cotacao = dados["data"]["currency"]
    timestamp = datetime.now()

    dados_transformados = BitcoinDados(
        valor=valor,
        criptomoeda=criptomoeda,
        cotacao=cotacao,
        timestamp=timestamp
    )

    return dados_transformados


def salvar_dados_sqlalchemy(dados):
    with Session() as session:
        session.add(dados)
        session.commit()
        print("Dados salvos no PostgreSQL!")


if __name__ == "__main__":
    while True:
        dados = extrair_dados_bitcoin()
        dados_transformados = transformar_dados_bitcoin(dados)
        print("Dados Tratados:")
        print(dados_transformados)
        salvar_dados_sqlalchemy(dados_transformados)
        print("Aguardando 15 segundos...")
        time.sleep(15)
