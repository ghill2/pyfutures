
# DEFAULT_INSTRUMENT_PROVIDER_CONFIG = InteractiveBrokersInstrumentProviderConfig(
#         chain_filters={
#             'FMEU': lambda x: x.contract.localSymbol[-1] not in ("M", "D"),
#         },
#         parsing_overrides={
#             "MIX": {
#                 "price_precision": 0,
#                 "price_increment": Price(5, 0),
#             },
#         },
# )

@pytest.fixture()
def provider_params():
    return dict(
        chain_filters={
            'FMEU': lambda x: x.contract.localSymbol[-1] not in ("M", "D"),
        },
        parsing_overrides={
            "MIX": {
                "price_precision": 0,
                "price_increment": Price(5, 0),
            },
        },
    )
