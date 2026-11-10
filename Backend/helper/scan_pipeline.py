"""A bounded fetch stage; uncommitted prefetched ranges are safe to replay."""
import asyncio


async def prefetch(reserve, fetch):
    queue = asyncio.Queue(maxsize=1)

    async def produce():
        try:
            while True:
                bounds = await reserve()
                if bounds is None:
                    break
                await queue.put((bounds, await fetch(bounds), None))
        except Exception as exc:
            await queue.put((None, None, exc))
        else:
            await queue.put((None, None, None))

    task = asyncio.create_task(produce())
    try:
        while True:
            bounds, messages, error = await queue.get()
            if error is not None:
                raise error
            if bounds is None:
                break
            yield bounds, messages
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
