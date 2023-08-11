import asyncio
import time
import telethon

async def process_message(msg: telethon.events.newmessage.NewMessage.Event):
    # Do something
    if msg.text == "Hello":
        for i in range(10):
            await msg.respond(f"Hello {i}")
            time.sleep(1)
    await msg.respond(f"Your msg is {msg.text}")