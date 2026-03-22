"""
Handler: Payment Created
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from db import get_redis_conn
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from orders.commands.write_order import modify_order

class PaymentCreatedHandler(EventHandler):
    """Handles PaymentCreated events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "PaymentCreated"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        order_id = event_data["order_id"]
        payment_id = event_data.get("payment_id") or order_id
        payment_link = event_data.get("payment_link", "")
        is_paid = event_data.get("is_paid", True)

        try:
            if not modify_order(order_id, is_paid, payment_id):
                raise RuntimeError(f"modify_order a échoué pour la commande {order_id}")
            r = get_redis_conn()
            r.hset(
                f"order:{order_id}",
                mapping={
                    "payment_link": payment_link,
                    "is_paid": str(is_paid),
                },
            )
            event_data['event'] = "SagaCompleted"
            self.logger.debug(f"payment_link={payment_link}")
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)

        except Exception as e:
            event_data['error'] = str(e)
            event_data['event'] = "PaymentCreationFailed"
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)


