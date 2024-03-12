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
take out nautilus references in enums
make test_kit parsing test
check historical bar timestamp order returned
support expiry date calculation by 3rd Tuesday of the month etc
support expiry date calculation by last day of the month 2 months previous to contract month
support lookback by market calendar if expiry date is within closed market hours
support cycle masking by year/month
support seasonal contracts with duplicate trading classes
support year range hold cycle cycle rules (EBM)


# Re-connect

Daily restarts are enforced by Trader Workstation.  
