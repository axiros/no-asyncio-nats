import no_asyncio_nats
import time
import secrets
import os
import datetime

data = secrets.token_bytes(10*1024)

def main():
    # Simple connection (backward compatible)
    nc = no_asyncio_nats.connect(
        "nats://localhost:4222",
        {"ping_interval": datetime.timedelta(20)}
    )

    start = time.monotonic()
    num = 5_000_000
    for _ in range(num):
        nc.publish('sub', data)
    nc.flush()

    delta = round(time.monotonic() - start, 2)
    print(num / delta, delta)

if __name__ == '__main__':
    main()
