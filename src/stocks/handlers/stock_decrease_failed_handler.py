"""
Handler: Stock Decrease Failed
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from typing import Dict, Any
import config
from event_management.base_handler import EventHandler
from orders.commands.order_event_producer import OrderEventProducer
from orders.commands.write_order import delete_order


class StockDecreaseFailedHandler(EventHandler):
    """Handles StockDecreaseFailed events"""
    
    def __init__(self):
        self.order_producer = OrderEventProducer()
        super().__init__()
    
    def get_event_type(self) -> str:
        """Get event type name"""
        return "StockDecreaseFailed"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        """Execute every time the event is published"""
        try:
            # La diminution du stock a échoué: compenser l'étape précédente (création de commande).
            deleted = delete_order(event_data["order_id"])
            if not deleted:
                raise Exception(f"Order {event_data['order_id']} not found or could not be deleted.")

            # Si la compensation a réussi, déclenchez OrderCancelled.
            event_data['event'] = "OrderCancelled"
        except Exception as e:
            # S'il est impossible de compenser, terminer la saga avec erreurs.
            event_data['error'] = str(e)
            event_data['event'] = "SagaCompleted"
        finally:
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
  
