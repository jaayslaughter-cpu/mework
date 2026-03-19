import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Construct the async PostgreSQL URL from environment variables
DB_USER = os.getenv("POSTGRES_USER", "propiq_admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "dev_password")
DB_NAME = os.getenv("POSTGRES_DB", "propiq_db")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")  # Docker service name

SQLALCHEMY_DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

engine = create_async_engine(SQLALCHEMY_DATABASE_URL, echo=False)

AsyncSessionLocal = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

# Dependency to inject DB session into routes
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
