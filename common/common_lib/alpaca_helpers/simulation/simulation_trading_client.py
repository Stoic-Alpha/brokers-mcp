import logging
from datetime import datetime
from typing import Union, Optional
from uuid import UUID

from alpaca.common import validate_uuid_id_param, validate_symbol_or_asset_id
from alpaca.trading.enums import OrderStatus
from alpaca.trading.requests import ReplaceOrderRequest, OrderRequest, GetOrderByIdRequest, LimitOrderRequest, \
    StopLossRequest, TakeProfitRequest, ClosePositionRequest
from sqlalchemy import literal, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from common_lib.alpaca_helpers.simulation.simulation_db_responses import CancelOrderResponse, FetchOrderResponse, \
    SubmitOrderResponse, ModifyOrderResponse
from common_lib.alpaca_helpers.simulation.simulation_trading_db import Order

logger = logging.getLogger(__name__)


class SimulationTradingClient:
    def __init__(self, db):
        self.db = db

    @staticmethod
    async def _get_order_by_id(session: AsyncSession, order_id: Union[UUID, str]) -> Order:
        """Fetch an order by ID from the database."""

        order_id = validate_uuid_id_param(order_id, "order_id")
        result = await session.execute(select(Order).where(Order.id == literal(order_id)))
        order = result.scalar_one_or_none()

        if not order:
            raise ValueError(f"Order with ID {order_id} not found")

        return order

    @staticmethod
    async def _create_order(session: AsyncSession, order_data: OrderRequest) -> Order:
        """Create a new order in the database."""

        entry_price = None
        if order_data.type.value == "limit" and order_data.limit_price:
            entry_price = order_data.limit_price

        side = getattr(order_data.side, 'value', None)

        new_order = Order(
            instrument=order_data.symbol,
            quantity=order_data.qty or 0,  # Ensure quantity is not None
            side=side,  # Convert enum to string
            entry_price=entry_price,
            take_profit_price=order_data.take_profit.limit_price if order_data.take_profit else None,
            stop_loss_price=order_data.stop_loss.stop_price if order_data.stop_loss else None,
            order_type=order_data.type.value,
            order_class=order_data.order_class.value if order_data.order_class else "simple",
            time_in_force=order_data.time_in_force.value,
            status=OrderStatus.NEW.value
            # extended_hours=bool(order_data.extended_hours),
            # client_order_id=order_data.client_order_id or str(uuid.uuid4()),
            # position_intent=order_data.position_intent.value if order_data.position_intent else None,
        )

        session.add(new_order)
        await session.commit()

        return new_order

    async def _prepare_new_order_request(self, existing_order,
                                         new_order_data: ReplaceOrderRequest) -> LimitOrderRequest:
        """Prepare a new LimitOrderRequest based on the existing order and new parameters."""
        new_order_request = LimitOrderRequest(
            symbol=existing_order.instrument,
            qty=new_order_data.qty if new_order_data.qty is not None else existing_order.quantity,
            side=existing_order.side,
            type=existing_order.order_type,
            time_in_force=new_order_data.time_in_force if new_order_data.time_in_force is not None else existing_order.time_in_force,
            limit_price=new_order_data.limit_price if new_order_data.limit_price is not None else existing_order.entry_price,
            order_class=existing_order.order_class
        )

        # Add take profit if it existed in the original order
        if existing_order.take_profit_price:
            new_order_request.take_profit = TakeProfitRequest(
                limit_price=existing_order.take_profit_price
            )

        # Add stop loss if provided in the new request
        if new_order_data.stop_price:
            new_order_request.stop_loss = StopLossRequest(
                stop_price=new_order_data.stop_price
            )

        return new_order_request

    async def submit_order(self, order_data: OrderRequest) -> SubmitOrderResponse:
        """Submit a new order."""
        async with self.db.SessionLocal() as session:
            new_order = await self._create_order(session, order_data)
            return SubmitOrderResponse(status=new_order.status, id=str(new_order.id))

    async def get_order_by_id(self, order_id: Union[UUID, str],
                              request: Optional[GetOrderByIdRequest] = None) -> FetchOrderResponse:
        """Retrieve an order by ID."""
        async with self.db.SessionLocal() as session:
            order = await self._get_order_by_id(session, order_id)
            return FetchOrderResponse(status=order.status, id=str(order.id))

    async def cancel_order_by_id(self, order_id: Union[UUID, str]) -> CancelOrderResponse:
        """Cancel an existing order by ID."""
        async with self.db.SessionLocal() as session:
            order = await self._get_order_by_id(session, order_id)
            order.status = OrderStatus.CANCELED.value
            order.updated_timestamp = datetime.utcnow()
            await session.commit()

            return CancelOrderResponse(id=str(order.id), status=order.status)

    async def replace_order_by_id(self, order_id: Union[UUID, str],
                                  new_order_data: ReplaceOrderRequest) -> ModifyOrderResponse:
        """Replace an existing order with a new one."""
        async with self.db.SessionLocal() as session:
            existing_order = await self._get_order_by_id(session, order_id)

            if existing_order.status == OrderStatus.CANCELED.value:
                raise ValueError(f"Order with ID {order_id} has already been canceled")

            # Cancel the existing order
            existing_order.status = OrderStatus.CANCELED.value
            existing_order.updated_timestamp = datetime.utcnow()

            # Prepare new order request using the private helper
            new_order_request = await self._prepare_new_order_request(existing_order, new_order_data)

            # Create the new order
            new_order = await self._create_order(session, new_order_request)

            # Return response
            return ModifyOrderResponse(
                status=new_order.status,
                id=str(new_order.id),
                qty=new_order.quantity,
                limit_price=new_order.entry_price if new_order.entry_price else None,
                stop_price=new_order.stop_loss_price if new_order.stop_loss_price else None
            )

    async def close_position(self, symbol_or_asset_id: Union[UUID, str],
                             close_options: Optional[ClosePositionRequest] = None) -> None:
        """Liquidate the position for a given asset."""
        symbol_or_asset_id = validate_symbol_or_asset_id(symbol_or_asset_id)

        if not close_options.percentage or close_options.percentage != "100":
            raise Exception("Can liquid only 100% of the given asset.")

        async with self.db.SessionLocal() as session:
            await session.execute(
                update(Order)
                .where(
                    Order.status == OrderStatus.NEW.value,
                    Order.instrument == literal(symbol_or_asset_id)  # Check for specific symbol
                )
                .values(
                    status=OrderStatus.FILLED.value,
                    updated_timestamp=datetime.utcnow()
                )
            )

            # Commit the changes
            await session.commit()
