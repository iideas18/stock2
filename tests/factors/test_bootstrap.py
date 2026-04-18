from instock.factors import bootstrap, registry


def test_bootstrap_registers_six_factors():
    registry.clear_registry()
    bootstrap._REGISTERED = False
    bootstrap.register_default_factors()
    names = set(registry.get_all().keys())
    # exact six names from Sub-1 MVP
    for expected in {"mom_20d", "pe_ttm", "pb", "roe_ttm",
                     "lhb_heat_30d", "north_holding_chg_5d"}:
        assert expected in names


def test_bootstrap_is_idempotent():
    registry.clear_registry()
    bootstrap._REGISTERED = False
    bootstrap.register_default_factors()
    bootstrap.register_default_factors()  # must not raise
    assert len(registry.get_all()) >= 6
