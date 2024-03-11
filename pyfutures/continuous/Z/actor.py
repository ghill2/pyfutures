from nautilus_trader.common.actor import Actor
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.continuous.chain import ContractChain


class ChainActor(Actor):
    def __init__(
        self,
        bar_type: BarType,
        chain: ContractChain,
    ):
        super().__init__()

        self._bar_type = bar_type
        self._chain = chain
        self._instrument_id = bar_type.instrument_id
        self._bar_spec = bar_type.spec
        self._aggregation_source = bar_type.aggregation_source

    @property
    def instrument_id(self) -> InstrumentId:
        return self._bar_type.instrument_id

    @property
    def bar_type(self) -> BarType:
        return self._bar_type

    @property
    def chain(self) -> ContractChain:
        return self._chain