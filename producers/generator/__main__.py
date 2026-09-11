from __future__ import annotations

import os
import random
import time

from producers.generator.events import env_chaos, next_batch
from producers.generator.producer import build_producer, produce_event, produce_poison
from streamcart.config import TOPICS
from streamcart.logging import log


def main() -> None:
    rate = float(os.getenv("GENERATOR_EVENTS_PER_SEC", "8"))
    seed = int(os.getenv("GENERATOR_SEED", "42"))
    chaos = env_chaos()
    rng = random.Random(seed)
    producer, encoders = build_producer()
    interval = 1.0 / max(rate, 0.1)
    log("info", "generator_start", rate=rate, chaos=chaos, seed=seed)

    try:
        while True:
            for event in next_batch(rng, chaos):
                produce_event(producer, encoders, event)
            if chaos and rng.random() < 0.02:
                produce_poison(producer, TOPICS["orders"])
            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        log("info", "generator_stop")
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
