from nautilus_trader.model.data import BarType


class ChainView:
    def __init__(self):
        pass

    @property
    def current_bar(self):
        return self.cache.bar(self.current_bar_type)

    @property
    def forward_bar(self):
        return self.cache.bar(self.forward_bar_type)

    @property
    def carry_bar(self):
        return self.cache.bar(self.carry_bar_type)

    @property
    def current_bar_type(self) -> BarType:
        return self._chain.current_bar_type(
            spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )

    @property
    def forward_bar_type(self) -> BarType:
        return self._chain.forward_bar_type(
            spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )

    @property
    def carry_bar_type(self) -> BarType:
        return self._chain.carry_bar_type(
            spec=self._bar_type.spec,
            aggregation_source=self._bar_type.aggregation_source,
        )
