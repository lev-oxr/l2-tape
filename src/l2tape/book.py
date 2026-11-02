"""Local L2 order book: price -> quantity on each side."""


class Book:
    def __init__(self, bids=(), asks=()):
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.apply(bids, asks)

    def apply(self, bids, asks) -> None:
        """An update carries the level's new quantity, not a change; quantity 0 removes it.

        Prices arrive as decimal strings; float(s) is exact enough to be a key
        (the same string always maps to the same float, checked on 8000 real prices).
        """
        for side, levels in ((self.bids, bids), (self.asks, asks)):
            for price, qty in levels:
                p, q = float(price), float(qty)
                if q:
                    side[p] = q
                else:
                    side.pop(p, None)

    def copy(self) -> "Book":
        return Book(self.bids.items(), self.asks.items())
