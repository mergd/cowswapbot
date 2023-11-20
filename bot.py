from typing import Dict, Any
import sqlite3

import uuid
import uvicorn
from fastapi import FastAPI, BackgroundTasks
from threading import Thread
import asyncio
import requests
import sqlite3
from eth_utils import is_address
from discord.ext import commands
import discord
from discord import app_commands
from pydantic import BaseModel
from dotenv import load_dotenv
import os


class Item(BaseModel):
    matchedReceipts: Any
    matchedTransactions: Any


def create_db():
    # Creates a connection to the SQLite database. It will create a new one if it doesn't exist.
    conn = sqlite3.connect('bot_data.db')
    # Creates a cursor object. This is what we use to execute commands on the database.
    c = conn.cursor()

    # Create table - addresses_channel
    c.execute('''
        CREATE TABLE IF NOT EXISTS addresses_channel
        (uuid TEXT PRIMARY KEY,
        address TEXT,
        channel_id STRING)
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS firehose_channels
        (channel_id STRING PRIMARY KEY)
    ''')

    # Save (commit) the changes and close the connection
    conn.commit()
    conn.close()


bot = commands.Bot(command_prefix="!", intents=discord.Intents.all())


@bot.tree.command(name="add_address", description="Add an Ethereum address to be tracked")
@app_commands.describe(address="The Ethereum address to be tracked")
async def add_address(interaction: discord.Interaction, address: str):
    try:
        if not is_address(address):
            await interaction.response.send_message("Invalid Ethereum address.")
            return

        conn = sqlite3.connect('bot_data.db')
        c = conn.cursor()
        new_uuid = uuid.uuid4()

        # Insert into channel_addresses
        c.execute("INSERT INTO addresses_channel VALUES (?, ?, ?)",
                  (str(new_uuid),  address, interaction.channel_id))

        conn.commit()
        conn.close()

        await interaction.response.send_message(f"Address {address} added for this channel.")
    except Exception as e:
        print(e)
        await interaction.response.send_message("Error adding address.")


@bot.tree.command(name="list_addresses", description="List all Ethereum addresses linked to this channel")
async def list_addresses(interaction: discord.Interaction):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()

    c.execute("SELECT address FROM addresses_channel WHERE channel_id = ?",
              (interaction.channel_id,))
    addresses = c.fetchall()
    conn.commit()
    conn.close()
    if addresses:
        addresses = "\n".join(address[0] for address in addresses)
        await interaction.response.send_message(f"Cowswap addresses tracked for this channel:\n{addresses}")
    else:
        await interaction.response.send_message("No addresses linked to this channel.")


@bot.tree.command(name="firehose", description="Get all cowswap transactions")
async def fire_hose(interaction: discord.Interaction):
    try:
        conn = sqlite3.connect('bot_data.db')
        c = conn.cursor()

        c.execute("SELECT * FROM firehose_channels WHERE channel_id=?",
                  (interaction.channel_id,))

        if interaction.channel_id is not None:
            await interaction.response.send_message("Fire hose already enabled.")
        else:
            c.execute("INSERT INTO firehose_channels VALUES ?",
                      interaction.channel_id)
            await interaction.response.send_message("Fire hose enabled.")

        conn.commit()
        conn.close()
    except Exception as e:
        print(e)
        await interaction.response.send_message("Fire hose not enabled.")


@bot.tree.command(name="stop_firehose", description="Stop getting all cowswap transactions")
async def stop_fire_hose(interaction: discord.Interaction):
    try:
        conn = sqlite3.connect('bot_data.db')
        c = conn.cursor()

        # Delete record
        c.execute("DELETE FROM firehose_channels WHERE channel_id = ?",
                  (interaction.channel_id))

        conn.commit()
        conn.close()
        await interaction.response.send_message("Fire hose disabled.")

    except Exception as e:
        print(e)
        await interaction.response.send_message("Fire hose not enabled.")


@bot.tree.command(name="remove_address", description="Remove an Ethereum address from being tracked")
@app_commands.describe(address="The address")
async def remove_address(interaction: discord.Interaction, address: str):
    try:
        conn = sqlite3.connect('bot_data.db')
        c = conn.cursor()

        c.execute("SELECT address FROM addresses_channel WHERE channel_id = ?",
                  (interaction.channel_id,))
        channel_addresses = c.fetchall()
        if interaction.channel_id in channel_addresses:
            c.execute("DELETE FROM addresses_channel WHERE channel_id = ? AND address = ?",
                      (interaction.channel_id, address,))
            await interaction.response.send_message(f"Address {address} removed for this channel.")
        else:
            await interaction.response.send_message("Address not found for this channel.")

        conn.commit()
        conn.close()

    except Exception as e:
        print(e)
        await interaction.response.send_message("Error removing address.")


async def send_message(channel, message):
    await channel.send(message)


async def handle_webhook(input: Item):
    raw_data = input.get("logs")
    filtered_data = [item for item in raw_data if item.get('topics')[0] ==
                     "0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17"]
    try:
        for orderFill in filtered_data:
            # Receiver address is the last 20 bytes of the second topic
            # \n Execution Price: {result.get('execution_priceUSD') :,} per {result.get('sellToken')}
            eth_address = '0x' + orderFill.get('topics')[1][-40:]
            result = parse_hex(orderFill.get('data'))
            etherscan_url = f"https://etherscan.io/tx/{orderFill.get('transactionHash')}"
            msg = f"""Order filled for {eth_address}:\n   
            \n
            Swap from: \n
            {result.get('sellAmount'):,} {result.get('sellToken')} ({result.get('sellUSD'):,} USD) for {result.get('buyAmount'):,} {result.get('buyToken')} ({result.get('buyUSD'):,} USD) at {result.get('execution_price')} ({result.get('execution_priceUSD')} USD) 
            

            \n Cowswap Fee: {result.get('feeAmount') :,} {result.get('sellToken')} ({result.get('feeAmountUSD')} USD) 
            \n View transaction here: {etherscan_url}"""

            print(msg)

            conn = sqlite3.connect('bot_data.db')
            c = conn.cursor()
            c.execute("SELECT * FROM firehose_channels")
            firehose_receivers = c.fetchall()
            if (firehose_receivers == None):
                continue
            else:
                for channel_id in firehose_receivers:
                    channel = bot.get_channel(channel_id)
                    sg = asyncio.run_coroutine_threadsafe(
                        send_message(channel=channel, message=msg), bot.loop)
                    sg.result()

            c.execute(
                "SELECT channel_id FROM addresses_channel WHERE address = ?", (eth_address,))
            channel_ids = c.fetchall()
            if (channel_ids == None):
                continue
            else:
                for channel_id in channel_ids:
                    print(channel_id)
                    channel = bot.get_channel(channel_id)

                    sg = asyncio.run_coroutine_threadsafe(
                        send_message(channel=channel, message=msg), bot.loop)
                    sg.result()
        conn.close()
    except Exception as e:
        print(e)
        pass
    return {"message": "Webhook received"}
# parse the data field of the topic area


def parse_hex(hex_string):
    # Remove the leading '0x'
    hex_string = hex_string[2:]

    # Split the hex string into 64-character chunks
    chunks = [hex_string[i:i+64] for i in range(0, len(hex_string), 64)]

    # Parse the chunks
    sell_token = '0x' + chunks[0][24:]
    buy_token = '0x' + chunks[1][24:]
    sell_amount = int(chunks[2], 16)
    buy_amount = int(chunks[3], 16)
    fee_amount = int(chunks[4], 16)

    # Identify token
    sell_token, sell_decimals, sell_usd = processToken(sell_token)
    buy_token, buy_decimals, buy_usd = processToken(buy_token)
    sell_amount = sell_amount / 10**sell_decimals
    buy_amount = buy_amount / 10**buy_decimals
    # cowswap takes fee from sell amount
    fee_amount = fee_amount / 10**sell_decimals

    execution_price = buy_amount / sell_amount
    if sell_usd is None:
        execution_priceUSD = round((buy_amount * buy_usd) / (sell_amount), 4)
    else:
        execution_priceUSD = 0
    # Find the order UUID
    uuid_start = hex_string.find('000038') + 6
    uuid_end = hex_string.find('0000', uuid_start)
    order_uuid = hex_string[uuid_start:uuid_end]

    return {
        'sellToken': sell_token,
        'buyToken': buy_token,
        'sellAmount': sell_amount,
        'buyUSD': round(buy_amount * buy_usd, 2),
        'sellUSD': round(sell_amount * sell_usd, 2),
        'feeAmountUSD': round(fee_amount * sell_usd, 2),
        'buyAmount': buy_amount,
        'feeAmount': fee_amount,
        'execution_price': round(execution_price, 4),
        'execution_priceUSD': execution_priceUSD,
        'orderUUID': order_uuid,
    }


def processToken(token):

    # query llamafi for price and name
    url = f'https://coins.llama.fi/prices/current/ethereum:{token}'
    print(url)
    response = requests.get(url)
    response.raise_for_status()

    res = response.json()['coins']
    keys = res.keys()

    if len(keys) == 0:
        return token, 18, 1
    else:
        key = list(keys)[0]
        return res[key]['symbol'], res[key]['decimals'], res[key]['price']


# Webserver thread
app = FastAPI()


@app.post("/webhook")
async def receive_webhook(item: Item, background_tasks: BackgroundTasks):
    background_tasks.add_task(handle_webhook, item.matchedReceipts[0])
    # asyncio.create_task(handle_webhook(item.matchedReceipts[0]))

    return {"message": "Webhook received"}


@app.post("/test")
async def test():
    channel = bot.get_channel(781679792667492375)
    if channel:
        await channel.send("test")
    else:
        print("Channel not found")


def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8000,)


if __name__ == "__main__":
    load_dotenv()
    create_db()
    TOKEN = os.getenv('BOT_TOKEN')
    fastapi_thread = Thread(target=run_fastapi)
    fastapi_thread.start()
    bot.run(TOKEN)
