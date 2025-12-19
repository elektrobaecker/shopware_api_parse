from shopware_bmecat.importer import calculate_gross


def test_calculate_gross_rounding():
    gross = calculate_gross(10.0, 0.19)
    assert gross == 11.9

    gross = calculate_gross(9.99, 0.19)
    assert gross == 11.89