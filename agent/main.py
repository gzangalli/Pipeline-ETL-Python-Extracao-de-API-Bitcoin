from agno.agent import Agent
from agno.models.groq import Groq
from agno.tools.postgres import PostgresTools
from dotenv import load_dotenv
import os

load_dotenv('../.env')

# Initialize PostgresTools with connection details
postgres_tools = PostgresTools(
    host=os.getenv("DATABASE_HOST"),
    port=os.getenv("DATABASE_PORT"),
    db_name=os.getenv("DATABASE_NAME"),
    user=os.getenv("DATABASE_USER"),
    password=os.getenv("DATABASE_PASSWORD"),
    table_schema="public",
)

# Create an agent with the PostgresTools
agent = Agent(tools=[postgres_tools],
              model=Groq(id="llama-3.3-70b-versatile"))

agent.print_response("Fale todas as tabelas do banco de dados", markdown=True)

agent.print_response("""
Faça uma query para pegar todas as cotações de bitcoin na tabela bitcoin_dados
""")

agent.print_response("""
Faça uma análise super complexa sobre o bitcoin usando os dados da tabela bitcoin_dados
""")