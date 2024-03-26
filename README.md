# pyfutures

An alternative implementation a `Interactive Brokers` trading adapter for nautilus_trader  

The goal is to support ONLY trading futures contracts on the `Interactive Brokers` exchange  

# notes

API > Settings > Enable ActiveX and Socket clients > Enabled  
API > Settings > Read-Only API > Disabled  
API > Settings > Socket Port > 4002  
API > Settings > Download Orders on Connection > Disabled  
API > Settings > Send instrument-specific attributes for dual-mode API client in > UTC format  

# TODO - ContinuousData

on_stop, save continuous and adjusted to cache
- if an exception is raised while the strategy is running it will stop and save the strategies state

on_start:

1) find chain position
    find current position on ib
    if current position can't be found, open a position based on roll calendar

2) reconcile data
    get the most recent month from continuous bar from the database
    set chain to most recent month
    
    request bars from previous, carry, forward, current and handle them on the actor
    
    if not enough time to roll
    the actor will roll 

    what if recent month is 

    check that current position matches position on ib
    create the missing continuous bars upto current time

NOTES:
    * what if the cached continuous bar history is lost?
    * what about finding the current position from the cached continuous bar history if ib position is not found?


creating missing continuous bars from the cached continuous bars to current time by requesting contract bars
- cache the continuous bars
- on startup, start month of the most recent continuous bar
- add continuous bar caching to the data module of maxlen

each continuous bar stores it's adjustment value





strategy reads from the cache and gets most recent continuous bar

--------------------------


Should the data module cache it's data in a database?


cache instruments every hour and after each roll?
create catalog with ib data append on startup, then request the last bit of data



on startup the strategy looks for the current position in the chain from IB.
if theres no position it will just open a new position based on the roll calendar



* add assertion to endtime in request data methods in client

* implement cache and delay for quote and trade ticks on client

request_bars is already using the cache and delays if not cached. need to do for quotes and trades.

* handle error in connection:

If this error is received by the connection the client should be set to disconnected.

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

* we don't need two caches for details and requests

# Re-connect

Daily restarts are enforced by Trader Workstation.
