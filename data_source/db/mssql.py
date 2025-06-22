import atexit
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env
load_dotenv()

# Get credentials and connection details
server = os.getenv("SERVER_ADDRESS", "localhost")
db_source = os.getenv("MSSQL_DB", "source")
username = os.getenv("MSSQL_USER")
password = os.getenv("MSSQL_PASSWORD")
port = os.getenv("MSSQL_NODE_PORT", "1433")
driver = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# Build connection URL
connection_url = (
    f"mssql+pyodbc://{username}:{password}@{server},{port}/{db_source}"
    f"?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
)

# SQLAlchemy engine
engine = create_engine(connection_url)


@atexit.register
def cleanup_engine():
    print("Disposing engine")
    engine.dispose()
