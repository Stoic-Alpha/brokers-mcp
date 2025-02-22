from alpaca.trading import OrderType, OrderClass, TimeInForce, OrderStatus
from sqlalchemy import String, DateTime, func, Integer, Float, Enum
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.orm import declarative_base
import uuid
from sqlalchemy.dialects.postgresql import UUID

Base = declarative_base()


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    instrument: Mapped[str] = mapped_column(String(20), nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)  # "BUY" or "SELL"
    entry_price: Mapped[float] = mapped_column(Float, nullable=False)
    take_profit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    stop_loss_price: Mapped[float | None] = mapped_column(Float, nullable=True)

    order_type: Mapped[str] = mapped_column(Enum(OrderType), nullable=False)  # Market, Limit, etc.
    order_class: Mapped[str] = mapped_column(Enum(OrderClass), nullable=False)  # Simple, Bracket, OTO
    time_in_force: Mapped[str] = mapped_column(Enum(TimeInForce), nullable=False)  # GTC, DAY, etc.
    status: Mapped[str] = mapped_column(Enum(OrderStatus), nullable=False, default=OrderStatus.ACCEPTED.value)

    created_timestamp: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now())
    updated_timestamp: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

class SimulationTradingDatabase:
    def __init__(self, db_url):
        self.engine = create_async_engine(db_url)
        self.SessionLocal = async_sessionmaker(bind=self.engine, class_=AsyncSession, expire_on_commit=False)

    async def get_session(self):
        """Get an async database session."""
        async with self.SessionLocal() as session:
            yield session

    async def create_tables(self):
        """Create tables asynchronously."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
