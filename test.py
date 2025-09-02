import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

url = (
    f"postgresql+psycopg://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}"
    f"@{os.getenv('PGHOST')}:{os.getenv('PGPORT')}/{os.getenv('PGDATABASE')}"
)

engine = create_engine(url)

with engine.connect() as conn:
    result = conn.execute(text("SELECT version();"))
    print(result.fetchone())