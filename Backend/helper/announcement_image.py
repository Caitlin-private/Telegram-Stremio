"""Bounded, in-memory poster framing for Telegram's chat preview."""
import asyncio
from io import BytesIO

import httpx
from PIL import Image, ImageOps

MAX_BYTES = 8 * 1024 * 1024
MAX_PIXELS = 12_000_000
FRAME_SIZE = 1280
PADDING = 32


def frame_poster(data: bytes) -> BytesIO:
    with Image.open(BytesIO(data)) as source:
        if source.width * source.height > MAX_PIXELS:
            raise ValueError('Announcement poster dimensions exceed the limit')
        # Contain, never fit/crop: all provider badges and title text survive.
        image = ImageOps.exif_transpose(source)
        image.thumbnail((FRAME_SIZE - PADDING * 2, FRAME_SIZE - PADDING * 2), Image.Resampling.LANCZOS)
        image = image.convert('RGBA')
        with Image.new('RGB', (FRAME_SIZE, FRAME_SIZE), (24, 24, 32)) as canvas:
            canvas.paste(image, ((FRAME_SIZE-image.width)//2, (FRAME_SIZE-image.height)//2), image)
            output = BytesIO()
            canvas.save(output, format='JPEG', quality=90)
        image.close()
    output.name = 'caitlin-poster.jpg'
    output.seek(0)
    return output


async def announcement_photo(url: str) -> BytesIO:
    async def download():
        async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
            async with client.stream('GET', url) as response:
                response.raise_for_status()
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(data) + len(chunk) > MAX_BYTES:
                        raise ValueError('Announcement poster download exceeds the limit')
                    data.extend(chunk)
                return bytes(data)
    data = await asyncio.wait_for(download(), timeout=25)
    # Image processing must not block ingestion/playback on the event loop.
    return await asyncio.to_thread(frame_poster, data)
