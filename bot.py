
import uvicorn
from fastapi import FastAPI, BackgroundTasks
from threading import Thread

import sqlite3
from eth_utils import is_address
from discord.ext import commands
import discord
from discord import app_commands
from pydantic import BaseModel
from dotenv import load_dotenv
import os
from typing import Dict, Any
import asyncio

bot = commands.Bot(command_prefix="!", intents=discord.Intents.all())


class Item(BaseModel):
    matchedReceipts: Any
    matchedTransactions: Any


@bot.event
async def on_ready():
    print("Bot is Up and Ready!")
    try:
        synced = await bot. tree. sync()
        print(f"Synced {len(synced)} command(s)")
    except Exception as e:
        print(e)


async def send_message(channel, message):
    try:
        await asyncio.wait_for(channel.send(message), timeout=5.0)
    except asyncio.TimeoutError:
        print('The message took failed to send')


channel_addresses = {}
firehose_receivers = []


@bot.tree.command(name="add_address", description="Add an Ethereum address to be tracked")
@app_commands.describe(address="The Ethereum address to be tracked")
async def add_address(interaction: discord.Interaction, address: str):
    if not is_address(address):
        await interaction.response.send_message("Invalid Ethereum address.")
        return

    if interaction.channel_id not in channel_addresses:
        channel_addresses[interaction.channel_id] = set()
    channel_addresses[interaction.channel_id].add(address)

    await interaction.response.send_message(f"Address {address} added for this channel.")


@bot.tree.command(name="list_addresses", description="List all Ethereum addresses linked to this channel")
async def list_addresses(interaction: discord.Interaction):
    # c.execute("SELECT address FROM addresses WHERE channel_id = ?",
    #           (interaction.channel_id,))
    # addresses = c.fetchall()
    addresses = channel_addresses.get(interaction.channel_id)

    if addresses:
        addresses = "\n".join(address[0] for address in addresses)
        await interaction.response.send_message(f"Cowswap addresses tracked for this channel:\n{addresses}")
    else:
        await interaction.response.send_message("No addresses linked to this channel.")


@bot.tree.command(name="firehose", description="Get all cowswap transactions")
async def fire_hose(interaction: discord.Interaction):
    firehose_receivers.append(interaction.channel_id)
    await interaction.response.send_message("Fire hose enabled.")


@bot.tree.command(name="stop_firehose", description="Stop getting all cowswap transactions")
async def stop_fire_hose(interaction: discord.Interaction):
    firehose_receivers.remove(interaction.channel_id)
    await interaction.response.send_message("Fire hose disabled.")


@bot.tree.command(name="remove_address", description="Remove an Ethereum address from being tracked")
@app_commands.describe(address="The address")
async def remove_address(interaction: discord.Interaction, address: str):
    # c.execute("DELETE FROM addresses WHERE channel_id = ? AND address = ?",
    #           (interaction.channel_id, address))
    # conn.commit()
    # delete from dictionary
    if interaction.channel_id in channel_addresses:
        channel_addresses[interaction.channel_id].discard(address)
        await interaction.response.send_message(f"Address {address} removed for this channel.")
    else:
        await interaction.response.send_message("Address not found for this channel.")


async def handle_webhook(input: Item):
    raw_data = input.get("logs")
    filtered_data = [item for item in raw_data if item.get('topics')[0] ==
                     "0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17"]
    for orderFill in filtered_data:
        # Receiver address is the last 20 bytes of the second topic
        eth_address = '0x' + orderFill.get('topics')[1][-40:]
        result = parse_hex(orderFill.get('data'))
        etherscan_url = f"https://etherscan.io/tx/{orderFill.get('transactionHash')}"
        msg = f"Order filled for {eth_address}:\n View transaction here: {etherscan_url}"

        # await bot.get_channel(781679792667492375).send(msg)
        asyncio.create_task(send_message(
            bot.get_channel(781679792667492375), msg))

        # if (firehose_receivers == None):
        #     continue
        # else:
        #     for channel_id in firehose_receivers:
        #         channel = bot.get_channel(channel_id)
        #         await channel.send(msg)
        # channel_ids = channel_addresses.get(eth_address)
        # if (channel_ids == None):
        #     continue
        # else:
        #     for channel_id in channel_ids:
        #         channel = bot.get_channel(channel_id[0])
        #         await channel.send(msg)

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
    sell_token, sell_decimals = processToken(sell_token)
    sell_amount = sell_amount / 10**sell_decimals
    buy_token, buy_decimals = processToken(buy_token)
    buy_amount = buy_amount / 10**buy_decimals
    # cowswap takes fee from sell amount
    fee_amount = fee_amount / 10**sell_decimals

    execution_price = buy_amount / sell_amount

    # Find the order UUID
    uuid_start = hex_string.find('000038') + 6
    uuid_end = hex_string.find('0000', uuid_start)
    order_uuid = hex_string[uuid_start:uuid_end]

    return {
        'sellToken': sell_token,
        'buyToken': buy_token,
        'sellAmount': sell_amount,
        'buyAmount': buy_amount,
        'feeAmount': fee_amount,
        'execution_price': execution_price,
        'orderUUID': order_uuid,
    }


def processToken(token):
    if token == '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee':
        return ('ETH', 18)
    elif token == '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48':
        return ('USDC', 6)
    elif token == '0xdAC17F958D2ee523a2206206994597C13D831ec7':
        return ('USDT', 6)
    elif token == '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599':
        return ('WBTC', 8)
    else:
        return (token, 18)


# Webserver thread
app = FastAPI()


@app.post("/webhook")
async def receive_webhook(item: Item, background_tasks: BackgroundTasks):
    background_tasks.add_task(handle_webhook, item.matchedReceipts[0])
    return {"message": "Webhook received"}


def run_fastapi():
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":

    load_dotenv()
    TOKEN = os.getenv('BOT_TOKEN')
    # Run FastAPI in a separate thread
    fastapi_thread = Thread(target=run_fastapi)
    fastapi_thread.start()

    bot.run(TOKEN)
