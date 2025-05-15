import requests
import time
import logging
from colorlog import ColoredFormatter
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv
import os
from datetime import datetime

# Configuração de logs coloridos
formatter = ColoredFormatter(
    "%(log_color)s%(asctime)s | %(levelname)-8s | %(message)s",
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
)

handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger = logging.getLogger("bitcoin_logger")
logger.setLevel(logging.INFO)
logger.handlers = [handler]

# Carrega variáveis de ambiente
load_dotenv('../')
DATABASE_URL = os.getenv("DATABASE_KEY")

# Configuração do banco
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


# Cria a tabela se não existir
try:
    Base.metadata.create_all(engine)
    logger.info("Tabela 'bitcoin_dados' verificada/criada com sucesso.")
except Exception as e:
    logger.error(f"Erro ao criar/verificar tabela no banco: {e}")


def extrair_dados_bitcoin():
    url = "https://api.coinbase.com/v2/prices/spot"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        logger.info("✅ Dados extraídos da API.")
        return response.json()
    except requests.RequestException as e:
        logger.warning(f"⚠️ Falha na requisição à API: {e}")
        return None


def transformar_dados_bitcoin(dados):
    try:
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

        logger.info(f"🔄 Dados transformados: {dados_transformados}")
        return dados_transformados
    except Exception as e:
        logger.error(f"Erro ao transformar dados: {e}")
        return None


def salvar_dados_sqlalchemy(dados):
    try:
        with Session() as session:
            session.add(dados)
            session.commit()
            logger.info("💾 Dados salvos no PostgreSQL.")
    except Exception as e:
        logger.error(f"Erro ao salvar dados no banco: {e}")


if __name__ == "__main__":
    logger.info("🚀 Iniciando coleta de dados do Bitcoin...")
    while True:
        dados = extrair_dados_bitcoin()
        if dados:
            dados_transformados = transformar_dados_bitcoin(dados)
            if dados_transformados:
                salvar_dados_sqlalchemy(dados_transformados)
        else:
            logger.warning("⚠️ Nenhum dado processado neste ciclo.")
        logger.info("⏳ Aguardando 15 segundos...\n")
        time.sleep(15)
