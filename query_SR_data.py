# -*- coding: utf-8 -*-
"""
Created on Sun Aug 22 10:04:21 2021

@author: kylej
"""

import pandas as pd
import numpy as np 
from datetime import datetime, timedelta, date, timezone
import time
from web3 import Web3
import json 
import requests
import os
from selenium import webdriver
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from tqdm import tqdm

def connect_mainnet(PROJECTID):
    """Connect to Eth mainnet using infura"""     
    w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/{}'.format(PROJECTID)))
    return w3

def get_transfer_events(w3, block_start, block_end, contract, token_contract_address, ABI):
    """
    Get all ERC-721 transfer events associated with a contract

    Parameters
    ----------
    w3 : 
        connection to mainnet.
    block_start : int
        starting block num to search for events.
    block_end : int
        end block num to search for events.
    contract : str
        eth address for contract.
    token_contract_address : 
        DESCRIPTION.
    ABI : json
        ABI for contract.

    Returns
    -------
    logs : dict
        dictionary of ERC-721 transfer events.

    """
    event_signature=w3.sha3(text="Transfer(address,address,uint256)").hex()
    # Get transfer events
    logs = w3.eth.getLogs({
        "fromBlock": block_start,
        "toBlock": block_end,
        "address": w3.toChecksumAddress(token_contract_address),
        "topics": [event_signature]
    })

    return logs

def decode_event_logs(logs,w3):
    """
    Decode transfer event logs
    
    Parameters
    ----------
    logs - raw event logs
    
    Returns Dictionary
    -------
    indexed by: txHash + tknid
    with: From, to, tokenID, blockNumber, transactionIndex

    """
    event_data = {}
    for i,event in enumerate(logs):    
        
        topic = Web3.toHex(event["topics"][0])
        
        #TRANSFERS
        if topic == w3.sha3(text="Transfer(address,address,uint256)").hex():
        
            from_ = "0x" + Web3.toHex(event["topics"][1])[26:]
            to_   = "0x" + Web3.toHex(event["topics"][2])[26:]
            #Token ID in topic or data?
            #SuperRare V1 Tokens in Topic
            #SuperRare V2 Tokens in Data
            try:
                tknid = Web3.toInt(event["topics"][3])
            except:
                tknid = int(event["data"],16)
            txhash = event["transactionHash"]
            blocknum = event["blockNumber"]
            transaction_index = event["transactionIndex"]    
            event_data[Web3.toInt(txhash)+tknid] = {"txhash":txhash, "from":from_,"to":to_,"tokenID":tknid,'blockNumber':blocknum,'transactionIndex':transaction_index}
        
    return event_data

def get_block_time(w3, block_num):
    """
    Get time of eth block

    Parameters
    ----------
    block_num : int
        ethereum block number.

    Returns
    -------
    blocktime : datetime
        datetime in UTC of block.

    """
    block_info = w3.eth.get_block(block_num)
    blocktime = datetime.fromtimestamp(block_info.timestamp,tz=timezone.utc)
    
    return blocktime

def get_transfer_data(w3, token_contract_address, ABI, block_increment):
    """
    Get Data on ERC-721 Transfers 
    
    Parameters
    ----------
    
    w3 - connection to mainnet 
    token_contract - address for platform
    ABI - 
    block_increment - how many blocks to search over
    
    Returns all ERC-721 Transfers decoded 
    -------
    indexed by: txHash + tknid
    with: From, to, tokenID, blockNumber, transactionIndex

    """
    dfs = []
    current_block = w3.eth.blockNumber
        
    #Get contract
    token_contract_address = Web3.toChecksumAddress(token_contract_address)
    contract = w3.eth.contract(token_contract_address, abi=ABI)
            
    i=0
    while i < current_block:
        block_start = i 
        block_end = i + block_increment
        if block_end > current_block:
            block_end = current_block
        print(block_start, block_end)
        i += block_increment
         
        #Get transfer events
        #event_signature = w3.sha3(text="Transfer(address,address,uint256)").hex()
        logs = get_transfer_events(w3,block_start, block_end,contract,token_contract_address,ABI)
        #Process log
        event_data = decode_event_logs(logs,w3)
        
        #Get dataframe of transfer events
        df_transfers = pd.DataFrame(event_data).transpose()
        dfs.append(df_transfers)

    df_transfers_all = pd.concat(dfs)

    return df_transfers_all

def get_creator_owners(df_transfers, contract_addresses, other_addresses=None):
    """
    Return cleaned data on transfers and creator/owner pairs
    Find current owner of each token and calculate number of tokens by address
    
    Parameters
    ----------
    df_transfers : pandas dataframe of ERC-721 transfers
    contract_addresses: contract addresses 
    other_addresses : optional, auction addresses etc. The default is None.

    Returns
    -------
    numtokensby_address : df
        Number of tokens held by each eth address
    df_creators_owners_noburn : df
        Creator/Current Owner (eth addresses) pairs for each token
    """
    #Sort on blocknumber and transaction index
    df_transfers = df_transfers.sort_values(["tokenID","blockNumber","transactionIndex"])
    
    #For each token, get the original creator and current owner
    minting_address = "0x0000000000000000000000000000000000000000"
    df_transfers["Creator"] = np.where(df_transfers["from"] == minting_address, df_transfers["to"], np.nan)
    df_transfers["CurrentOwner"] = df_transfers.groupby("tokenID")["to"].transform("last")
 
    #Only creators and owners
    df_transfers["Creator"] = df_transfers.groupby("tokenID")["Creator"].ffill()    
    df_creators_owners = df_transfers.drop_duplicates("tokenID")
    df_creators_owners.index = df_creators_owners.tokenID

    #Remove burned tokens 
    burn_address = "0x000000000000000000000000000000000000dead"
    df_creators_owners_noburn = df_creators_owners[df_creators_owners.CurrentOwner != burn_address]

    #Remove tokens held by the contract  
    not_contract = (~df_creators_owners_noburn.CurrentOwner.isin(contract_addresses))
    df_creators_owners_noburn = df_creators_owners_noburn[not_contract][["Creator","CurrentOwner","tokenID","contract_address"]]
    
    #Not owned by auction address or minting address
    df_creators_owners_noburn = df_creators_owners_noburn[df_creators_owners_noburn.CurrentOwner != other_addresses]
    df_creators_owners_noburn = df_creators_owners_noburn[df_creators_owners_noburn.CurrentOwner != minting_address]
    
    #Remove tokens held by the creator still (?)
    #not_creator_owned = (df_creators_owners_noburn.Creator != df_creators_owners_noburn.CurrentOwner)
    #df_creators_owners_noburn = df_creators_owners_noburn[not_creator_owned]
    
    #Number of tokens by address
    numtokensby_address = df_creators_owners_noburn.groupby("CurrentOwner").tokenID.count()
    
    return numtokensby_address, df_creators_owners_noburn

def get_opensea_account_name(ETH_ADDRESS):
    """
    Query OpenSea API for account info on Ethereum address
    
    Parameters
    ----------
    ETH_ADDRESS : str
        Ethereum address to query OpenSea API

    Returns
    -------
    adr_acct_name : str
        name of account associated with ETH ADDRESS

    """
    url = "https://superrare.com/{}".format(ETH_ADDRESS)
    
    try:
        response = requests.request("GET", url)
        
        response.html.find('#profile__username').text
        
        soup = BeautifulSoup(response.text, 'html.parser')
        adr_acct_name = soup.find("div", {"class": "AccountHeader--title"}).contents[0]
    except:
        adr_acct_name=ETH_ADDRESS[:5]+"..."
        
    return adr_acct_name
    
def get_superrare_account_name(unique_eth_addresses):
    """
    Query for account info on Ethereum address
    
    Parameters
    ----------
    ETH_ADDRESS : str
        Ethereum address to query OpenSea API

    Returns
    -------
    adr_acct_name : str
        name of account associated with ETH ADDRESS

    """
    
    #get all unique eth addresses
    #unique_eth_addresses = list(set(superrare_creator_owners["Creator"].unique().tolist() + superrare_creator_owners["CurrentOwner"].unique().tolist()))
    
    #set up browser
    p_chromedriver = r"C:\Users\kylej\Downloads\chromedriver.exe"
    opts = Options()
    opts.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36")
    opts.add_argument("--headless")  
    
    #Start Selenium Webdriver - Change Path as Needed 
    browser = webdriver.Chrome(executable_path = p_chromedriver, chrome_options=opts)
    
    #Usernames / addresses 
    df_address_usernames = pd.DataFrame()
    for ETH_ADDRESS in tqdm(unique_eth_addresses):
        
        #go to url
        url = "https://superrare.com/{}".format(ETH_ADDRESS)    
        try:
            browser.get(url)
            time.sleep(1)  
            username=browser.find_element_by_xpath('//*[@id="root"]/div/div/div[3]/div/div[2]/div/span').text
            followers=browser.find_element_by_xpath('//*[@id="root"]/div/div/div[3]/div/div[1]/div[2]/p[1]').text  
            
            df_address_usernames.loc[ETH_ADDRESS,"Username"] = username
            df_address_usernames.loc[ETH_ADDRESS,"Followers"] = int(followers.replace("Followers: ",""))
            
        except:  
            df_address_usernames.loc[ETH_ADDRESS,"Username"] = ETH_ADDRESS                        
                             
    return df_address_usernames

def get_eth_balance(w3, ETH_ADDRESS):
    """
    Get balance in ETH of address
    
    Parameters
    ----------
    w3 : connection to eth mainnet
        
    ETH_ADDRESS : string
        non-checksum address.

    Returns
    -------
    eth_bal : float
        current balance for address.

    """
    
    #Convert to checksum address
    balance = w3.eth.get_balance(w3.toChecksumAddress(ETH_ADDRESS))
    eth_bal = w3.fromWei(balance, 'ether')

    return eth_bal

def get_tx_value(w3, txhash, platform):
    """
    Get value of last transaction of coins currently owned by that address

    Parameters
    ----------
    w3: conneciton to eth mainnet 
        
    txhash: str
        transaction hash

    Returns
    -------
    tx_value : float
        total value of ERC-721 transfer transaction

    """
    
    eth_total=np.nan
    tx = w3.eth.getTransactionReceipt(txhash)
    gas_used = tx["gasUsed"]
    
    for event in tx["logs"]:    
        topic = w3.toHex(event["topics"][0])
    
        ######## SuperRare ########
        #Sold(index_topic_1 address _buyer, index_topic_2 address _seller, uint256 _amount, index_topic_3 uint256 _tokenId)
        if topic == w3.sha3(text="Sold(address,address,uint256,uint256)").hex():    
            from_ = "0x" + Web3.toHex(event["topics"][1])[26:]
            to_   = "0x" + Web3.toHex(event["topics"][2])[26:]
            tknid = Web3.toInt(event["topics"][3])
            eth_total = int(event["data"][:66],16)/1e18
        
        #AcceptBid (index_topic_1 address _bidder, index_topic_2 address _seller, uint256 _amount, index_topic_3 uint256 _tokenId)
        elif topic == "0xd6deddb2e105b46d4644d24aac8c58493a0f107e7973b2fe8d8fa7931a2912be":
            eth_total = int(event["data"],16)/1e18
        
        #Auction Won
        elif topic == "0xea6d16c6bfcad11577aef5cc6728231c9f069ac78393828f8ca96847405902a9": 
            from_ = "0x" + Web3.toHex(event["topics"][1])[26:]
            to_   = "0x" + Web3.toHex(event["topics"][2])[26:]
            tknid = Web3.toInt(event["topics"][3])
            eth_total = int(event["data"][66:],16)/1e18
        
        #Bought from
        elif topic == "0x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9": 
            from_ = "0x" + Web3.toHex(event["topics"][3])[26:]
            to_   = "0x" + Web3.toHex(event["topics"][2])[26:]
            eth_total = int(event["data"][:66],16)/1e18
            tknid     = int(event["data"][66:],16)
        
        #Accepted an offer
        elif topic == "0x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6": 
            from_ = "0x" + Web3.toHex(event["topics"][3])[26:]
            to_   = "0x" + Web3.toHex(event["topics"][2])[26:]
            tknid     = int(event["data"][66:],16)
            eth_total = int(event["data"][:66],16)/1e18
         
        ######## Foundation
        #Foundation - auction settled on primary market (ACTUAL VALUE FROM IS 1.15x BECAUSE OF FND FEES)
        #ReserveAuctionFinalized (index_topic_1 uint256 auctionId, index_topic_2 address seller, index_topic_3 address bidder, uint256 f8nFee, uint256 creatorFee, uint256 ownerRev)
        elif topic==w3.sha3(text="ReserveAuctionFinalized(uint256,address,address,uint256,uint256,uint256)").hex():
            from_ = "0x" + Web3.toHex(event["topics"][2])[26:]
            to_   = "0x" + Web3.toHex(event["topics"][3])[26:]
            tknid     = None
            eth_total = (int(event["data"][66*2:],16)/1e18 )/0.85
        
        ######## KnownOrigin
        #Purchase (index_topic_1 uint256 _tokenId, index_topic_2 uint256 _editionNumber, index_topic_3 address _buyer, uint256 _priceInWei)
        elif topic==w3.sha3(text="Purchase(uint256,uint256,address,uint256)").hex():
            eth_total = int(event["data"],16)/1e18
        
        #Bid Accepted
        #BidAccepted (index_topic_1 address _bidder, index_topic_2 uint256 _editionNumber, index_topic_3 uint256 _tokenId, uint256 _amount)
        elif topic==w3.sha3(text="BidAccepted(address,uint256,uint256,uint256)").hex():
            eth_total = int(event["data"],16)/1e18
        
        #Secondary Market - token purchased 
        #TokenPurchased(index_topic_1 uint256 _tokenId, index_topic_2 address _buyer, index_topic_3 address _seller, uint256 _price)
        elif topic==w3.sha3(text="TokenPurchased(uint256,address,address,uint256)").hex():
            eth_total = int(event["data"],16)/1e18
        
        ######## MakersPlace
        
        elif topic == "0xfc8d57c890a29ac7508080b26d7187224039062b525f377f0c7746193c59baa8":
            eth_total = int(event["data"][194:194+64],16)/1e18
       
        ######## ASYNC
        #TokenSale (uint256 tokenId, uint256 salePrice, address buyer)
        elif topic==w3.sha3(text="TokenSale(uint256,uint256,address)").hex():
            eth_total = int(event["data"][66:66*2-2],16)/1e18
        
        ###########################
        #Orders matched - OpenSea
        #OrdersMatched (bytes32 buyHash, bytes32 sellHash, index_topic_1 address maker, index_topic_2 address taker, uint256 price, index_topic_3 bytes32 metadata)
        elif topic==w3.sha3(text="OrdersMatched(bytes32,bytes32,address,address,uint256,bytes32)").hex():
            from_ = "0x" + Web3.toHex(event["topics"][1])[26:]
            to_   = "0x" + Web3.toHex(event["topics"][2])[26:]
            tknid     = None
            eth_total = int(event["data"][66*2:],16)/1e18 
        
    #Get gas price of transaction
    #tx_transaction = w3.eth.getTransaction(txhash)
    #gas_price = tx_transaction["gasPrice"]
    #gas_price_eth = Web3.fromWei(gas_price, 'ether')
    #total_gas_value = gas_used*gas_price_eth
    
    return eth_total

def main():
    
    #Initialize
    PROJECTID = "92cdded0532248a580b662b97e2d51f2"    
    w3 = connect_mainnet(PROJECTID)
    
    #####################################
    ### SUPERRARE 
    #####################################   
    
    ### ERC-721 TRANSFER EVENTS #########
    for version in [1,2]:
        if version == 1: 
            token_contract_address = '0x41a322b28d0ff354040e2cbc676f0320d8c8850d'
            ABI = json.loads('[{"constant":false,"inputs":[{"name":"_uri","type":"string"},{"name":"_editions","type":"uint256"},{"name":"_salePrice","type":"uint256"}],"name":"addNewTokenWithEditions","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_tokenId","type":"uint256"},{"name":"_salePrice","type":"uint256"}],"name":"setSalePrice","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"_name","type":"string"}],"payable":false,"stateMutability":"pure","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_tokenId","type":"uint256"}],"name":"approve","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"currentBidDetailsOfToken","outputs":[{"name":"","type":"uint256"},{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"approvedFor","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"acceptBid","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_creator","type":"address"}],"name":"isWhitelisted","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"bid","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"tokensOf","outputs":[{"name":"","type":"uint256[]"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_percentage","type":"uint256"}],"name":"setMaintainerPercentage","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_creator","type":"address"}],"name":"whitelistCreator","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_uri","type":"string"}],"name":"originalTokenOfUri","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"_symbol","type":"string"}],"payable":false,"stateMutability":"pure","type":"function"},{"constant":false,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"cancelBid","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"salePriceOfToken","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_tokenId","type":"uint256"}],"name":"transfer","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"takeOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_percentage","type":"uint256"}],"name":"setCreatorPercentage","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"creatorOfToken","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"buy","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"constant":false,"inputs":[{"name":"_uri","type":"string"}],"name":"addNewToken","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"creatorPercentage","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"maintainerPercentage","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_creator","type":"address"}],"name":"WhitelistCreator","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_bidder","type":"address"},{"indexed":true,"name":"_amount","type":"uint256"},{"indexed":true,"name":"_tokenId","type":"uint256"}],"name":"Bid","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_bidder","type":"address"},{"indexed":true,"name":"_seller","type":"address"},{"indexed":false,"name":"_amount","type":"uint256"},{"indexed":true,"name":"_tokenId","type":"uint256"}],"name":"AcceptBid","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_bidder","type":"address"},{"indexed":true,"name":"_amount","type":"uint256"},{"indexed":true,"name":"_tokenId","type":"uint256"}],"name":"CancelBid","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_buyer","type":"address"},{"indexed":true,"name":"_seller","type":"address"},{"indexed":false,"name":"_amount","type":"uint256"},{"indexed":true,"name":"_tokenId","type":"uint256"}],"name":"Sold","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_tokenId","type":"uint256"},{"indexed":true,"name":"_price","type":"uint256"}],"name":"SalePriceSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"previousOwner","type":"address"},{"indexed":true,"name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_from","type":"address"},{"indexed":true,"name":"_to","type":"address"},{"indexed":false,"name":"_tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_owner","type":"address"},{"indexed":true,"name":"_approved","type":"address"},{"indexed":false,"name":"_tokenId","type":"uint256"}],"name":"Approval","type":"event"}]') 
            df_transfers_SR_V1 = get_transfer_data(w3, token_contract_address, ABI, block_increment=100000)
            df_transfers_SR_V1["contract_address"] = token_contract_address 
            
        elif version == 2:
            token_contract_address = '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0'
            ABI = json.loads('[{"constant":true,"inputs":[{"name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_enabled","type":"bool"}],"name":"enableWhitelist","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"to","type":"address"},{"name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"from","type":"address"},{"name":"to","type":"address"},{"name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_tokenId","type":"uint256"},{"name":"_uri","type":"string"}],"name":"updateTokenMetadata","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"owner","type":"address"},{"name":"index","type":"uint256"}],"name":"tokenOfOwnerByIndex","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_address","type":"address"}],"name":"isWhitelisted","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"tokenCreator","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"from","type":"address"},{"name":"to","type":"address"},{"name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"index","type":"uint256"}],"name":"tokenByIndex","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_tokenId","type":"uint256"}],"name":"deleteToken","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"renounceOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_removedAddress","type":"address"}],"name":"removeFromWhitelist","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"isOwner","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"to","type":"address"},{"name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_whitelistees","type":"address[]"}],"name":"initWhitelist","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"from","type":"address"},{"name":"to","type":"address"},{"name":"tokenId","type":"uint256"},{"name":"_data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"_uri","type":"string"}],"name":"addNewToken","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"_newAddress","type":"address"}],"name":"addToWhitelist","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"name":"owner","type":"address"},{"name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"inputs":[{"name":"_name","type":"string"},{"name":"_symbol","type":"string"},{"name":"_oldSuperRare","type":"address"}],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_tokenId","type":"uint256"},{"indexed":false,"name":"_uri","type":"string"}],"name":"TokenURIUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_newAddress","type":"address"}],"name":"AddToWhitelist","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"_removedAddress","type":"address"}],"name":"RemoveFromWhitelist","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"previousOwner","type":"address"},{"indexed":true,"name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":true,"name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"approved","type":"address"},{"indexed":true,"name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"operator","type":"address"},{"indexed":false,"name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"}]')
            df_transfers_SR_V2 = get_transfer_data(w3, token_contract_address, ABI, block_increment=100000)
            df_transfers_SR_V2["contract_address"] = token_contract_address
            
    df_transfers_SR = pd.concat([df_transfers_SR_V1,df_transfers_SR_V2])
    df_transfers_SR["Platform"]="SuperRare"
    
    #SuperRare addresses 
    contract_addresses = ["0x41a322b28d0ff354040e2cbc676f0320d8c8850d","0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0"]
    auction_address = "0x8c9f364bf7a56ed058fc63ef81c6cf09c833e656"
    #Get current owners 
    superrare_numtokens_byadr, superrare_creator_owners = get_creator_owners(df_transfers_SR, contract_addresses, other_addresses=auction_address)
    #Get number owned / created by address
    superrare_creator_owners["NumTokensOwned"] = superrare_creator_owners.groupby("CurrentOwner").tokenID.transform('count')
    superrare_creator_owners["NumTokensCreated"] = superrare_creator_owners.groupby("Creator").tokenID.transform('count') 
    
    #load previous mapping 
    address_name_mapping = pd.read_csv(r"C:\Users\kylej\Documents\GitHub\SuperRare-Network\superrare_username_to_address_2021-08-22.csv")
    superrare_creator_owners2 = pd.merge(superrare_creator_owners,address_name_mapping,left_on="Creator",right_on="Address",how='left')
    superrare_creator_owners3 = pd.merge(superrare_creator_owners2,address_name_mapping,left_on="CurrentOwner",right_on="Address",how='left')
    superrare_creator_owners3.drop(["Address_x","Address_y"],axis=1,inplace=True)
    superrare_creator_owners3.columns =  ['ArtistAdr', 'CollectorAdr', 'tokenID', 'contract_address',
       'NumTokensOwnedCollector', 'NumTokensCreatedArtist', 'ArtistName', 'ArtistFollowers',
       'CollectorName', 'CollectorFollowers']
  
    #get new artists/collectors 
    no_name_artists = superrare_creator_owners3[(superrare_creator_owners3.ArtistName.isnull())]["ArtistAdr"].unique().tolist()
    no_name_collectors = superrare_creator_owners3[(superrare_creator_owners3.CollectorName.isnull())]["CollectorAdr"].unique().tolist()
    addresses_ = no_name_artists+no_name_collectors
    new_address_names = get_superrare_account_name(addresses_)
    new_address_names["Address"] = new_address_names.index
    new_address_names.rename(columns={"Username":"Name"},inplace=True)
    
    #add new collectors / artists 
    address_name_mapping2 = pd.concat([address_name_mapping,new_address_names])
    
    #merge on
    superrare_creator_owners2 = pd.merge(superrare_creator_owners,address_name_mapping2,left_on="Creator",right_on="Address",how='left')
    superrare_creator_owners3 = pd.merge(superrare_creator_owners2,address_name_mapping2,left_on="CurrentOwner",right_on="Address",how='left')
    
    superrare_creator_owners3.drop(["Address_x","Address_y"],axis=1,inplace=True)
    superrare_creator_owners3.columns =  ['ArtistAdr', 'CollectorAdr', 'tokenID', 'contract_address',
       'NumTokensOwnedCollector', 'NumTokensCreatedArtist', 'ArtistName', 'ArtistFollowers',
       'CollectorName', 'CollectorFollowers']
    
    p_out = r"C:\Users\kylej\Documents\GitHub\SuperRare-Network"  
    superrare_creator_owners3.to_csv(os.path.join(p_out, "superrare top artists and collectors_2021-08-29.csv"),index=False)
    
