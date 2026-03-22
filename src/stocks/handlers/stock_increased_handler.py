"""
Handler: Stock Decreased
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from db import get_sqlalchemy_session
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from stocks.commands.write_stock import check_in_items_to_stock, update_stock_redis


class StockIncreasedHandler(EventHandler):
    """Handles StockIncrease events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "StockIncreased"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        try:
            # Compensation: remettre le stock (annulation après échec paiement).
            session = get_sqlalchemy_session()
            check_in_items_to_stock(session, event_data["order_items"])
            session.commit()
            # Garder le read-model (Redis) en phase avec MySQL.
            update_stock_redis(event_data["order_items"], "+")

            # Si l'operation a réussi, déclenchez OrderCancelled.
            event_data['event'] = "OrderCancelled"
        except Exception as e:
            if "session" in locals():
                session.rollback()
            # Si la compensation échoue, terminer la saga avec erreurs.
            event_data['error'] = str(e)
            event_data['event'] = "SagaCompleted"
        finally:
            if "session" in locals():
                session.close()
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)



