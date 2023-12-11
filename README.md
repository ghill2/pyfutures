# pyfutures

An alternative implementation a `Interactive Brokers` trading adapter for nautilus_trader  

The goal is to support ONLY trading futures contracts on the `Interactive Brokers` exchange  

# notes

API > Settings > Enable ActiveX and Socket clients > Enabled  
API > Settings > Read-Only API > Disabled  
API > Settings > Socket Port > 4002  
API > Settings > Download Orders on Connection > Disabled  
API > Settings > Send instrument-specific attributes for dual-mode API client in > UTC format  

# TODO
client: refactor end of handshake  
client: self._handshake: Future > Event  
client: handle writing to closed transport  

# Re-connect

Daily restarts are enforced by Trader Workstation.  