from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, Boolean, DateTime
import datetime
import os

PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT_EXTERNAL")

DATABASE_URL = f"postgresql+asyncpg://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

Base = declarative_base()

class Project(Base):
    __tablename__ = "projects"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    api_key = Column(String, unique=True, index=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.datetime.now(datetime.timezone.utc))

# Dependency для FastAPI
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session