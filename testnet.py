import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
from web3 import Web3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Headers for API requests
OPENSEA_API_KEY = os.getenv("f239fdf5f14849c19d202d49b5716f5d")
HEADERS = {
    "Authorization": f"Bearer {OPENSEA_API_KEY}"
}

# Cache for fetched data
CACHE = {}
CACHE_EXPIRATION = timedelta(minutes=10)  # Cache TTL

# Ethereum setup
INFURA_PROJECT_ID = os.getenv("eb4cb697e8cd4a24844c0512b6acacca")
w3 = Web3(Web3.HTTPProvider(f"https://goerli.infura.io/v3/{INFURA_PROJECT_ID}"))
wallet_address = os.getenv("0x745461ae3ee10F26e314735b6AF8ee41cD313E2d")
private_key = os.getenv("6f1ab0cab66a32ab2c83a7df59dd3ae2a4bf36f254341d2ab25aa29430a43f1f")
RATE_LIMIT_DELAY = 1  # Delay between API calls (in seconds)

PROFIT_THRESHOLD = 0.2  # Minimum profit margin (20%)
SPENDING_LIMIT_PERCENTAGE = 0.25  # Maximum percentage of wallet balance to spend

def fetch_trending_nfts():
    """
    Fetch trending NFTs from the API with caching to reduce API calls.
    Dynamically filters based on wallet's spending power.
    """
    global CACHE
    current_time = datetime.now()

    # Check cache validity
    if 'trending_nfts' in CACHE and (current_time - CACHE['last_fetched'] < CACHE_EXPIRATION):
        logging.info("Using cached trending NFT data.")
        return CACHE['trending_nfts']

    # Determine wallet's spending power
    wallet_balance = get_wallet_balance()
    max_spending_limit = wallet_balance * SPENDING_LIMIT_PERCENTAGE

    logging.info(f"Wallet balance: {wallet_balance} ETH, spending limit: {max_spending_limit} ETH.")

    # Fetch data from API
    url = "https://api.opensea.io/api/v1/assets"
    params = {
        "order_direction": "desc",
        "offset": 0,
        "limit": 50,
        "price_min": 0,
        "price_max": max_spending_limit
    }
    nft_list = []

    while True:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code == 200:
            data = response.json()
            assets = data['assets']
            if not assets:
                break  # Exit when no more assets
            for asset in assets:
                traits = {trait['trait_type']: trait['value'] for trait in asset['traits']}
                floor_price = asset.get('sell_orders', [{}])[0].get('current_price', None)
                nft_list.append({
                    "name": asset['name'],
                    "token_id": asset['token_id'],
                    "collection": asset['collection']['name'],
                    "contract_address": asset['asset_contract']['address'],  # Dynamically add contract address
                    "floor_price": float(floor_price) / 1e18 if floor_price else None,  # Convert wei to ETH
                    "traits": traits
                })
            params['offset'] += 50  # Fetch the next page
            time.sleep(RATE_LIMIT_DELAY)  # Respect rate limit
        else:
            logging.error(f"API call failed with status code: {response.status_code}")
            break

    # Store data in cache
    nft_data = pd.DataFrame(nft_list)
    CACHE['trending_nfts'] = nft_data
    CACHE['last_fetched'] = current_time
    logging.info("Fetched new trending NFT data from API.")
    return nft_data

def filter_by_traits(df, desired_traits):
    """
    Filter NFTs for specific traits.
    Args:
        df (DataFrame): NFT data.
        desired_traits (dict): Traits to prioritize (e.g., {"Background": "Gold"}).
    Returns:
        DataFrame: Filtered NFTs.
    """
    filtered = df[df['traits'].apply(lambda traits: all(item in traits.items() for item in desired_traits.items()))]
    return filtered

def calculate_profitability(df):
    """
    Calculate potential profitability for NFTs based on floor price and historical trends.
    Args:
        df (DataFrame): NFT data with floor prices.
    Returns:
        DataFrame: Data with profitability metrics.
    """
    df = df.copy()
    df['potential_profit'] = df['floor_price'] * (1 + PROFIT_THRESHOLD)  # Hypothetical resale price (20% increase)
    df['profit_margin'] = df['potential_profit'] - df['floor_price']
    return df

def get_wallet_balance():
    """
    Fetch the current wallet balance.
    Returns:
        float: Wallet balance in ETH.
    """
    balance = w3.eth.get_balance(wallet_address)
    return Web3.fromWei(balance, 'ether')

def execute_buy(token_id, price):
    """
    Execute a buy transaction for an NFT if within the wallet spending limit.
    Args:
        token_id (int): Token ID of the NFT.
        price (float): Price in ETH.
    Returns:
        str: Transaction hash or error message.
    """
    wallet_balance = w3.eth.get_balance(wallet_address)
    price_in_wei = Web3.toWei(price, 'ether')
    spending_limit = wallet_balance * SPENDING_LIMIT_PERCENTAGE

    if price_in_wei > spending_limit:
        logging.error(f"Price {price} ETH exceeds spending limit of {Web3.fromWei(spending_limit, 'ether')} ETH.")
        return "Exceeds spending limit"

    transaction = {
        'from': wallet_address,
        'value': price_in_wei,
        'gas': 200000,
        'gasPrice': w3.toWei('50', 'gwei'),
        'nonce': w3.eth.getTransactionCount(wallet_address)
    }
    signed_tx = w3.eth.account.sign_transaction(transaction, private_key=private_key)
    tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
    logging.info(f"Executed buy for token_id {token_id}, tx_hash: {tx_hash.hex()}.")
    logging.info(f"Updated wallet balance: {get_wallet_balance()} ETH.")
    return tx_hash.hex()

def relist_nft(token_id, resale_price, contract_address):
    """
    Relist an NFT for sale on OpenSea.
    Logs details about the relist attempt.
    Args:
        token_id (int): Token ID of the NFT.
        resale_price (float): Desired resale price in ETH.
        contract_address (str): Contract address of the NFT.
    Returns:
        str: Confirmation or error message.
    """
    sell_order_payload = {
        "asset": {
            "token_id": token_id,
            "token_address": contract_address
        },
        "start_amount": resale_price,
        "expiration_time": int(time.time()) + 86400  # 24-hour listing
    }
    response = requests.post(
        "https://testnets-api.opensea.io/v2/orders/post",
        headers=HEADERS,
        json=sell_order_payload
    )
    if response.status_code == 200:
        logging.info(f"Relisted NFT: Token ID {token_id}, Price {resale_price} ETH, Contract {contract_address}")
        return response.json()
    else:
        logging.error(f"Failed to relist NFT: Token ID {token_id}, Price {resale_price} ETH, Contract {contract_address}. Status code: {response.status_code}, Response: {response.text}")
        return response.text

def monitor_and_trade():
    """
    Monitor trends, execute trades, and relist purchased NFTs for resale.
    Runs in a perpetual loop until stopped.
    """
    try:
        while True:
            logging.info("Starting trend monitoring...")
            trending_nfts = fetch_trending_nfts()

            if trending_nfts is not None:
                logging.info(f"Retrieved {len(trending_nfts)} NFTs.")

                # Example: Filter NFTs by traits
                desired_traits = {"Background": "Gold"}  # Replace with actual desired traits
                filtered_nfts = filter_by_traits(trending_nfts, desired_traits)
                logging.info(f"Found {len(filtered_nfts)} NFTs with desired traits.")

                # Calculate profitability
                profitable_nfts = calculate_profitability(filtered_nfts)
                profitable_nfts = profitable_nfts[profitable_nfts['profit_margin'] > 0]
                logging.info(f"Identified {len(profitable_nfts)} profitable NFTs.")

                # Execute buy and relist for the most profitable NFT (example logic)
                if not profitable_nfts.empty:
                    top_nft = profitable_nfts.iloc[0]
                    tx_hash = execute_buy(top_nft['token_id'], top_nft['floor_price'])
                    if tx_hash != "Exceeds spending limit":
                        logging.info(f"Purchased NFT: Token ID {top_nft['token_id']}, Price {top_nft['floor_price']} ETH")
                        # Relist the NFT for a profit
                        resale_price = top_nft['potential_profit']
                        relist_response = relist_nft(top_nft['token_id'], resale_price, top_nft['contract_address'])

            logging.info("Monitoring cycle complete. Waiting for the next interval...")
            time.sleep(60)  # Wait for 1 minute before the next cycle
    except KeyboardInterrupt:
        logging.info("Bot stopped by user.")

if __name__ == "__main__":
    monitor_and_trade()
