"""
Handler: Stock Decreased
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer


class StockDecreasedHandler(EventHandler):
    """Handles StockDecreased events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "StockDecreased"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        try:
            # Après la diminution du stock, simuler la création d'une transaction de paiement
            # (l'intégration complète Payments/Outbox est traitée dans les activités suivantes).
            payment_id = event_data.get("payment_id") or event_data["order_id"]
            event_data["payment_link"] = (
                f"http://api-gateway:8080/payments-api/payments/process/{payment_id}"
            )
            event_data["is_paid"] = True
            event_data["event"] = "PaymentCreated"
            self.logger.debug(f"payment_link={event_data['payment_link']}")
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
        except Exception as e:
            event_data['error'] = str(e)
            event_data['event'] = "PaymentCreationFailed"
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
