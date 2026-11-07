import json
import logging
import os
import queue
import signal
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pyotp
import requests
from azure.eventhub import EventData, EventHubProducerClient
from dotenv import load_dotenv
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# Load Producer/.env for local runs. Explicit process variables still win.
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

# ---------- config (all from env) ----------
API_KEY = os.environ["ANGEL_API_KEY"]
CLIENT_CODE = os.environ["ANGEL_CLIENT_CODE"]
PIN = os.environ["ANGEL_PIN"]
TOTP_SECRET = os.environ["ANGEL_TOTP_SECRET"]

EVENTHUB_CONN_STR = os.environ["EVENTHUB_CONN_STR"]
EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME")  # optional if conn str has EntityPath

SYMBOLS = [s.strip().upper() for s in os.environ.get(
    "SYMBOLS", "RELIANCE,TCS,INFY,HDFCBANK,ICICIBANK,SBIN,ITC,LT,AXISBANK,KOTAKBANK"
).split(",")]
SCRIP_MASTER_URL = os.environ.get(
    "SCRIP_MASTER_URL",
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
)
PARTITION_BUCKETS = int(os.environ.get("PARTITION_BUCKETS", "8"))
FLUSH_INTERVAL_S = float(os.environ.get("FLUSH_INTERVAL_S", "1.0"))
MAX_BATCH_EVENTS = int(os.environ.get("MAX_BATCH_EVENTS", "500"))
ALWAYS_RUN = os.environ.get("ALWAYS_RUN", "false").lower() == "true"

IST = timezone(timedelta(hours=5, minutes=30))
NSE_CM = 1  # exchangeType for NSE cash
MODE_QUOTE = 2

events_q: "queue.Queue[dict]" = queue.Queue(maxsize=200_000)
stop = threading.Event()
token_to_symbol: dict[str, str] = {}
stats = {"received": 0, "sent": 0, "dropped_queue": 0, "dropped_send": 0}


def in_market_hours() -> bool:
    now = datetime.now(IST)
    if now.weekday() >= 5:  # Sat/Sun (exchange holidays NOT handled)
        return False
    return (9, 0) <= (now.hour, now.minute) <= (15, 35)


def load_universe() -> list[str]:
    data = requests.get(SCRIP_MASTER_URL, timeout=60).json()
    wanted = set(SYMBOLS)
    for row in data:
        if row.get("exch_seg") == "NSE" and row.get("symbol", "").endswith("-EQ"):
            if row.get("name", "").upper() in wanted:
                token_to_symbol[str(row["token"])] = row["name"].upper()
    missing = wanted - set(token_to_symbol.values())
    if missing:
        log.warning("symbols not found in scrip master: %s", sorted(missing))
    log.info("universe loaded: %d instruments", len(token_to_symbol))
    return list(token_to_symbol.keys())


# ---------- sender thread: queue -> Event Hubs ----------
def flush(producer: EventHubProducerClient, events: list[dict]) -> None:
    groups = defaultdict(list)
    for e in events:
        groups[int(e["token"]) % PARTITION_BUCKETS].append(e)

    for bucket, items in groups.items():
        key = str(bucket)
        for attempt in range(3):
            try:
                batch = producer.create_batch(partition_key=key)
                for e in items:
                    payload = EventData(json.dumps(e))
                    try:
                        batch.add(payload)
                    except ValueError:  # batch full
                        producer.send_batch(batch)
                        batch = producer.create_batch(partition_key=key)
                        batch.add(payload)
                producer.send_batch(batch)
                stats["sent"] += len(items)
                break
            except Exception as ex:
                log.error("send failed (attempt %d): %s", attempt + 1, ex)
                time.sleep(2 ** attempt)
        else:
            stats["dropped_send"] += len(items)


def sender_loop() -> None:
    kwargs = {"conn_str": EVENTHUB_CONN_STR}
    if EVENTHUB_NAME:
        kwargs["eventhub_name"] = EVENTHUB_NAME
    producer = EventHubProducerClient.from_connection_string(**kwargs)
    buf: list[dict] = []
    last_flush = time.time()
    last_log = time.time()
    with producer:
        while not (stop.is_set() and events_q.empty() and not buf):
            try:
                buf.append(events_q.get(timeout=0.2))
            except queue.Empty:
                pass
            due = time.time() - last_flush >= FLUSH_INTERVAL_S
            if buf and (len(buf) >= MAX_BATCH_EVENTS or due):
                flush(producer, buf)
                buf, last_flush = [], time.time()
            if time.time() - last_log >= 30:
                log.info("stats %s queue=%d", stats, events_q.qsize())
                last_log = time.time()


# ---------- websocket session ----------
def build_event(msg: dict) -> dict | None:
    token = str(msg.get("token", "")).strip('"')
    if not token:
        return None
    return {
        "source": "angelone",
        "token": token,
        "symbol": token_to_symbol.get(token),
        "exchange_type": msg.get("exchange_type"),
        "seq_no": msg.get("sequence_number"),
        "exchange_ts_ms": msg.get("exchange_timestamp"),
        "ingest_ts_ms": int(time.time() * 1000),
        "ltp_paise": msg.get("last_traded_price"),
        "last_qty": msg.get("last_traded_quantity"),
        "avg_price_paise": msg.get("average_traded_price"),
        "volume_day": msg.get("volume_trade_for_the_day"),
        "total_buy_qty": msg.get("total_buy_quantity"),
        "total_sell_qty": msg.get("total_sell_quantity"),
        "open_paise": msg.get("open_price_of_the_day"),
        "high_paise": msg.get("high_price_of_the_day"),
        "low_paise": msg.get("low_price_of_the_day"),
        "close_paise": msg.get("closed_price"),
    }


def run_session(tokens: list[str]) -> None:
    smart = SmartConnect(api_key=API_KEY)
    session = smart.generateSession(CLIENT_CODE, PIN, pyotp.TOTP(TOTP_SECRET).now())
    if not session or not session.get("status"):
        raise RuntimeError(f"login failed: {session}")
    auth_token = session["data"]["jwtToken"]
    feed_token = smart.getfeedToken()

    sws = SmartWebSocketV2(auth_token, API_KEY, CLIENT_CODE, feed_token)

    def on_open(wsapp):
        log.info("websocket open, subscribing %d tokens", len(tokens))
        sws.subscribe("stream1", MODE_QUOTE, [{"exchangeType": NSE_CM, "tokens": tokens}])

    def on_data(wsapp, message):
        stats["received"] += 1
        if not isinstance(message, dict):
            return
        ev = build_event(message)
        if ev is None:
            return
        try:
            events_q.put_nowait(ev)
        except queue.Full:
            stats["dropped_queue"] += 1

    def on_error(wsapp, error):
        log.error("ws error: %s", error)

    def on_close(wsapp, *args):
        log.warning("ws closed")

    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = on_error
    sws.on_close = on_close

    def watchdog():
        while not stop.is_set():
            if not ALWAYS_RUN and not in_market_hours():
                log.info("market closed, closing socket")
                break
            time.sleep(30)
        try:
            sws.close_connection()
        except Exception:
            pass

    threading.Thread(target=watchdog, daemon=True).start()
    sws.connect()  # blocks until closed


def main() -> None:
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    tokens = load_universe()
    if not tokens:
        raise SystemExit("no tokens resolved; check SYMBOLS / scrip master")

    sender = threading.Thread(target=sender_loop, daemon=True)
    sender.start()

    backoff = 5
    while not stop.is_set():
        if not ALWAYS_RUN and not in_market_hours():
            time.sleep(60)
            continue
        try:
            run_session(tokens)
            backoff = 5
        except Exception as ex:
            log.exception("session failed: %s", ex)
            backoff = min(backoff * 2, 300)
        if not stop.is_set():
            log.info("reconnecting in %ds", backoff)
            stop.wait(backoff)

    sender.join(timeout=30)
    log.info("shutdown complete, final stats %s", stats)


if __name__ == "__main__":
    main()
