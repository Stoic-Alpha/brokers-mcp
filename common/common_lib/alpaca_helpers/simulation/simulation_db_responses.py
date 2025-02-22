from alpaca.trading import OrderStatus
from pydantic import BaseModel
from uuid import UUID

class SubmitOrderResponse(BaseModel):
    status: OrderStatus
    id: UUID | str | None

class FetchOrderResponse(BaseModel):
    status: OrderStatus
    id: UUID | str | None

class CancelOrderResponse(BaseModel):
    status: OrderStatus
    id: UUID | str | None

class ModifyOrderResponse(BaseModel):
    status: OrderStatus
    id: UUID | str | None
    qty: float | None
    limit_price: float | None
    stop_price: float | None
