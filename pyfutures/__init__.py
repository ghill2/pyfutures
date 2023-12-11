from dotenv import dotenv_values

PROXY_EMAIL = dotenv_values().get("PROXY_EMAIL")
PROXY_PASSWORD = dotenv_values().get("PROXY_PASSWORD")
IB_ACCOUNT_ID = dotenv_values().get("IB_ACCOUNT_ID")
IB_USERNAME = dotenv_values().get("IB_USERNAME")
IB_PASSWORD = dotenv_values().get("IB_PASSWORD")