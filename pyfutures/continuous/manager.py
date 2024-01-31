

        
class ChainManager:
    
    def __init__(
        self,
        bar_type: BarType,
        config: ContractChainConfig,
    ):
        
        self._current_details: ChainDetails = None  # initialized on start
    
    @property
    def chain(self) -> ContractChain:
        return self._chain
    
    
    
    