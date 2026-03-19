import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import URL

# Construct the async PostgreSQL URL from environment variables
DB_USER = os.getenv("POSTGRES_USER", "propiq_admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "dev_password")
DB_NAME = os.getenv("POSTGRES_DB", "propiq_db")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")  # Docker service name

SQLALCHEMY_DATABASE_URL = URL.create(
    drivername="postgresql+asyncpg",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=5432,
    database=DB_NAME
)

engine = create_async_engine(SQLALCHEMY_DATABASE_URL, echo=False)

AsyncSessionLocal = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

# Dependency to inject DB session into routes
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
