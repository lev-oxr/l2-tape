from l2tape.book import Book


def test_update_replaces_quantity_and_zero_removes_level():
    book = Book(bids=[("67000.0", "2.0"), ("66999.9", "1.0")], asks=[("67000.1", "0.5")])
    book.apply(bids=[("67000.0", "1.5"), ("66999.9", "0")], asks=[("67000.2", "3")])

    assert book.bids == {67000.0: 1.5}  # 1.5 replaces 2.0, not 3.5; 66999.9 is gone
    assert book.asks == {67000.1: 0.5, 67000.2: 3.0}


def test_removing_a_level_that_is_not_there_is_fine():
    # Binance docs: deleting an unknown level "can happen and is normal".
    book = Book()
    book.apply(bids=[("1.0", "0")], asks=[])
    assert book.bids == {}
