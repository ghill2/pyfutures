INCOMING_MESSAGE_SPEC = {
    1: "priceSizeTick",
    2: "tickSize",
    3: "orderStatus",
    4: "errorMsg",
    5: "openOrder",
    6: "updateAccountValue",
    7: "updatePortfolio",
    8: "updateAccountTime",
    9: "nextValidId",
    10: "contractDetails",
    11: "execDetails",
    12: "updateMktDepth",
    13: "updateMktDepthL2",
    14: "updateNewsBulletin",
    15: "managedAccounts",
    16: "receiveFA",
    17: "historicalData",
    18: "bondContractDetails",
    19: "scannerParameters",
    20: "scannerData",
    21: "tickOptionComputation",
    45: "tickGeneric",
    46: "tickString",
    47: "tickEFP",
    49: "currentTime",
    50: "realtimeBar",
    51: "fundamentalData",
    52: "contractDetailsEnd",
    53: "openOrderEnd",
    54: "accountDownloadEnd",
    55: "execDetailsEnd",
    56: "deltaNeutralValidation",
    57: "tickSnapshotEnd",
    58: "marketDataType",
    59: "commissionReport",
    61: "position",
    62: "positionEnd",
    63: "accountSummary",
    64: "accountSummaryEnd",
    65: "verifyMessageAPI",
    66: "verifyCompleted",
    67: "displayGroupList",
    68: "displayGroupUpdated",
    69: "verifyAndAuthMessageAPI",
    70: "verifyAndAuthCompleted",
    71: "positionMulti",
    72: "positionMultiEnd",
    73: "accountUpdateMulti",
    74: "accountUpdateMultiEnd",
    75: "securityDefinitionOptionParameter",
    76: "securityDefinitionOptionParameterEnd",
    77: "softDollarTiers",
    78: "familyCodes",
    79: "symbolSamples",
    80: "mktDepthExchanges",
    81: "tickReqParams",
    82: "smartComponents",
    83: "newsArticle",
    84: "tickNews",
    85: "newsProviders",
    86: "historicalNews",
    87: "historicalNewsEnd",
    88: "headTimestamp",
    89: "histogramData",
    90: "historicalDataUpdate",
    91: "rerouteMktDataReq",
    92: "rerouteMktDepthReq",
    93: "marketRule",
    94: "pnl",
    95: "pnlSingle",
    96: "historicalTicks",
    97: "historicalTicksBidAsk",
    98: "historicalTicksLast",
    99: "tickByTick",
    100: "orderBound",
    101: "completedOrder",
    102: "completedOrdersEnd",
    103: "replaceFAEnd",
    104: "wshMetaData",
    105: "wshEventData",
    106: "historicalSchedule",
    107: "userInfo",
}

"""
market order filled
    b"5\x005\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.79\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x005\x001\x002138440174\x000\x000\x000\x00\x002138440174.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.79\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00"
    b"3\x005\x00Submitted\x000\x001\x000\x002138440174\x000\x000\x001\x00\x000\x00"
    b"11\x00-1\x005\x00623496135\x00R\x00FUT\x0020231227\x000.0\x00\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x000000e9b5.6555a859.01.01\x0020231116-12:15:35\x00DU1234567\x00ICEEU\x00BOT\x001\x0096.79\x002138440174\x001\x000\x001\x0096.79\x005\x00\x00\x00\x001\x00"
    b"5\x005\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.79\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x005\x001\x002138440174\x000\x000\x000\x00\x002138440174.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.79\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00"
    b"3\x005\x00Filled\x001\x000\x0096.79\x002138440174\x000\x0096.79\x001\x00\x000\x00"
    # b'5\x005\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.79\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x005\x001\x002138440174\x000\x000\x000\x00\x002138440174.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7\x00\x00\x00GBP\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.79\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00'
    # b'3\x005\x00Filled\x001\x000\x0096.79\x002138440174\x000\x0096.79\x001\x00\x000\x00'
    b"59\x001\x000000e9b5.6555a859.01.01\x001.7\x00GBP\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00"

limit_order_filled
    b"5\x003\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.9\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x003\x001\x002138440172\x000\x000\x000\x00\x002138440172.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00PreSubmitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.9\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00"
    b"3\x003\x00PreSubmitted\x000\x001\x000\x002138440172\x000\x000\x001\x00\x000\x00"
    b"11\x00-1\x003\x00623496135\x00R\x00FUT\x0020231227\x000.0\x00\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x000000e9b5.6555a7e7.01.01\x0020231116-12:07:51\x00DU1234567\x00ICEEU\x00BOT\x001\x0096.90\x002138440172\x001\x000\x001\x0096.90\x003\x00\x00\x00\x001\x00"
    b"5\x003\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.9\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x003\x001\x002138440172\x000\x000\x000\x00\x002138440172.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.9\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00"
    b"3\x003\x00Filled\x001\x000\x0096.90\x002138440172\x000\x0096.90\x001\x00\x000\x00"
    b"5\x003\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00MKT\x0096.9\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x003\x001\x002138440172\x000\x000\x000\x00\x002138440172.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Filled\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7\x00\x00\x00GBP\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0097.9\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00"
    b"3\x003\x00Filled\x001\x000\x0096.90\x002138440172\x000\x0096.90\x001\x00\x000\x00"
    b"59\x001\x000000e9b5.6555a7e7.01.01\x001.7\x00GBP\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00"
    
limit order accepted
    b"5\x0029\x00623496135\x00R\x00FUT\x0020231227\x000\x00?\x001000\x00ICEEU\x00GBP\x00RZ3\x00R\x00BUY\x001\x00LMT\x0087.16\x000.0\x00GTC\x00\x00DU1234567\x00\x000\x0029\x001\x002138440195\x000\x000\x000\x00\x002138440195.0/DU1234567/100\x00\x00\x00\x00\x00\x00\x00\x00\x00\x000\x00\x00-1\x000\x00\x00\x00\x00\x00\x002147483647\x000\x000\x000\x00\x003\x000\x000\x00\x000\x000\x00\x000\x00None\x00\x000\x00\x00\x00\x00?\x000\x000\x00\x000\x000\x00\x00\x00\x00\x00\x000\x000\x000\x002147483647\x002147483647\x00\x00\x000\x00\x00IB\x000\x000\x00\x000\x000\x00Submitted\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x00\x00\x00\x00\x00\x000\x000\x000\x00None\x001.7976931348623157E308\x0088.16\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x001.7976931348623157E308\x000\x00\x00\x00\x000\x001\x000\x000\x000\x00\x00\x000\x00\x00\x00\x00\x00\x00"
    b"3\x0029\x00Submitted\x000\x001\x000\x002138440195\x000\x000\x001\x00\x000\x00"
    
submit_order invalid quantity

    b"4\x002\x0015\x0010318\x00This order doesn't support fractional quantity trading\x00\x00"
    
    
"""