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
handle error in connection:
    b'\x00\x00\x00n4\x002\x00-1\x002110\x00Connectivity between Trader Workstation and server is broken. It will be restored automatically.\x00\x00'
    1970-01-01T00:00:00 [DBG] Connection : <-- b'\x00\x00\x00n4\x002\x00-1\x002110\x00Connectivity between Trader Workstation and server is broken. It will be restored automatically.\x00\x00'
1970-01-01T00:00:00 [INF] Connection : API connection ready, server version 176
1970-01-01T00:00:00 [DBG] Connection : <-- b'\x00\x00\x0094\x002\x00-1\x002103\x00Market data farm connection is broken:hfarm\x00\x00\x00\x00\x00=4\x002\x00-1\x002103\x00Market data farm connection is broken:usfarm.nj\x00\x00\x00\x00\x0094\x002\x00-1\x002103\x00Market data farm connection is broken:jfarm\x00\x00\x00\x00\x00<4\x002\x00-1\x002103\x00Market data farm connection is broken:usfuture\x00\x00\x00\x00\x00<4\x002\x00-1\x002103\x00Market data farm connection is broken:cashfarm\x00\x00\x00\x00\x00<4\x002\x00-1\x002103\x00Market data farm connection is broken:eufarmnj\x00\x00\x00\x00\x00:4\x002\x00-1\x002103\x00Market data farm connection is broken:usfarm\x00\x00\x00\x00\x0084\x002\x00-1\x002105\x00HMDS data farm connection is broken:euhmds\x00\x00\x00\x00\x0084\x002\x00-1\x002105\x00HMDS data farm connection is broken:hkhmds\x00\x00'
1970-01-01T00:00:00 [DBG] Connection : <-- b'\x00\x00\x00:4\x002\x00-1\x002105\x00HMDS data farm connection is broken:fundfarm\x00\x00\x00\x00\x0084\x002\x00-1\x002105\x00HMDS data farm connection is broken:ushmds\x00\x00\x00\x00\x00=4\x002\x00-1\x002157\x00Sec-def data farm connection is broken:secdefil\x00\x00'

handle resubscribe:
    There is also a message (1101) that tells you that subscriptions were lost and you have to resubscribe.

connection:
    add maintenance schedule to disable/enable connectivity
    
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
