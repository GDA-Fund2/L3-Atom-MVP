from helpers.util import create_lob_event, create_market_order

import json

class NormaliseOkex():
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    def normalise(self, data) -> dict:
        lob_events = []
        market_orders = []

        # If the message is not a trade or a book update, ignore it. This can be seen by if the JSON response contains an "event" key.
        if 'event' in data:
            print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS

        # Handling new LOB events
        if data['arg']['channel'] == 'books':
            order_data = data['data'][0]
            ts = float(order_data['ts'])
            for ask in order_data['asks']:
                price = float(ask[0])
                no_orders = int(ask[3])
                size = float(ask[1])
                # For Okex, if the number of orders is 0, it means that the level is being removed
                if no_orders == 0:
                    lob_action = 3
                    if price in self.ACTIVE_LEVELS:
                        self.ACTIVE_LEVELS.remove(price)
                    self.QUOTE_NO += 1
                # If the price is already in the active levels, it means that the level is being updated with a new size
                elif price in self.ACTIVE_LEVELS:
                    lob_action = 4
                # Otherwise, it means that a new price level is being inserted
                else:
                    lob_action = 2
                    self.ACTIVE_LEVELS.add(price)
                # Once the nature of the lob event has been determined, it can be created and added to the list of lob events
                lob_events.append(create_lob_event(
                    quote_no=self.QUOTE_NO,
                    event_no=self.EVENT_NO,
                    order_id=self.ORDER_ID,
                    side=2,
                    price=price,
                    size=size if size else -1,
                    lob_action=lob_action,
                    send_timestamp=ts,
                    receive_timestamp=data["receive_timestamp"],
                    order_type=0
                ))
                self.QUOTE_NO += 1
                self.ORDER_ID += 1
            for bid in order_data['bids']:
                price = float(bid[0])
                no_orders = int(bid[3])
                size = float(bid[1])
                if no_orders == 0:
                    lob_action = 3
                    if price in self.ACTIVE_LEVELS:
                        self.ACTIVE_LEVELS.remove(price)
                    self.QUOTE_NO += 1
                elif price in self.ACTIVE_LEVELS:
                    lob_action = 4
                else:
                    lob_action = 2
                    self.ACTIVE_LEVELS.add(price)
                lob_events.append(create_lob_event(
                    quote_no=self.QUOTE_NO,
                    event_no=self.EVENT_NO,
                    order_id=self.ORDER_ID,
                    side=1,
                    price=price,
                    size=size if size else -1,
                    lob_action=lob_action,
                    send_timestamp=ts,
                    receive_timestamp=data["receive_timestamp"],
                    order_type=0
                ))
                self.QUOTE_NO += 1
                self.ORDER_ID += 1

        elif data['arg']['channel'] == 'trades':
            trade = data['data'][0]
            # Okex only sends one trade per message, so we can simply extract the data and create a market order
            market_orders.append(create_market_order(
                order_id=self.ORDER_ID,
                price=float(trade['px']),
                trade_id=trade['tradeId'],
                timestamp=float(trade['ts']),
                side=1 if trade['side'] == 'buy' else 2,
                size = float(trade['sz'])
            ))
            self.ORDER_ID += 1

        # If the data is in an unexpected format, ignore it
        else:
            print(f"Received unrecognised message {json.dumps(data)}")
            return self.NO_EVENTS
        self.EVENT_NO += 1

        # Creating final normalised data dictionary which will be returned to the Normaliser
        normalised = {
            "lob_events": lob_events,
            "market_orders": market_orders
        }

        return normalised
