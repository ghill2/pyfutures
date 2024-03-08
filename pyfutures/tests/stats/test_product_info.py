from pyfutures.stats.product_info import MarginInfo
from pyfutures.tests.test_kit import IBTestProviderStubs


m_info = MarginInfo()
rows = m_info.sort_by_margin(IBTestProviderStubs.universe_rows())
print(rows[0])
print(rows[1])
