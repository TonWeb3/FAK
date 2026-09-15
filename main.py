import asyncio
import time
import json
import os
import math
import re
from datetime import datetime
from typing import Dict, Any, Optional, List

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import httpx

from bot.config import settings, CONFIG_PATH
import bot.data as data
import bot.ws_data as ws_data
import bot.chainlink as chainlink
import bot.indicators as indicators
import bot.engines as engines
import bot.utils as utils
from bot.clob_trader import clob_trader

STATE_PATH = "state_data.json"
SIGNALS_PATH = os.path.join("logs", "signals.csv")
TELEGRAM_SUBS_PATH = "telegram_subscribers.json"

# ── WebSocket broadcast clients for real-time live PnL & dashboard ────────────
_ws_clients = set()

# ── Event-driven early entries & wakeups ───────────────────────────────────────
MIN_EVAL_INTERVAL_S = 0.25
CTX_MAX_AGE_S = 4.0
POLY_WS_MAX_AGE_MS = 2500
MARK_CAPTURE_WINDOW_MS = 20_000

_market_event = asyncio.Event()
_entry_lock = asyncio.Lock()
_last_eval_ts = 0.0

def _wake_entry(data_payload=None):
    _market_event.set()

# ── Redemption state ──────────────────────────────────────────────────────────
REDEEM_CHECK_INTERVAL_S = 60
REDEEM_SUBMITTED_HIDE_S = 15 * 60
_redeem_wake = asyncio.Event()
_redeem_lock = asyncio.Lock()

# ── Global state ───────────────────────────────────────────────────────────────
state = {
    "latest_data": {},
    "last_update_ts": 0,
    "trading_mode": settings.MODE,
    "paper_balance": settings.PAPER_BALANCE_USD,
    "active_trades": [],
    "trade_history": [],
    "logs": [],
    "log_seq": 0,
    "last_trade_side": None,
    "last_balance_refresh": 0,
    # Trading is OFF until user presses Start.
    "running": False,
    "market_opens": {},
    "last_window_start": None,
    "last_seen_price": None,
    # Capital extractor
    "withdraw_state": "idle",  # "idle" | "in_progress" | "cooldown"
    "last_withdrawal": None,
    "withdraw_flat_since": None,
    "withdraw_submitted_at": None,
    "withdraw_locked_market": None,
    # Telegram
    "telegram_subscribers": [],
    # Event-driven execution
    "trade_ctx": None,
    "event_exec": None,
    # Redeem tracking
    "redeemable": {"positions": [], "count": 0, "value": 0.0, "checked_at": None, "error": None},
    "redeem_run": {"busy": False, "total": 0, "done": 0, "ok": 0, "failed": 0, "errors": [], "value": 0.0, "finished_at": None},
    "redeem_submitted": {},
}

def log_message(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {msg}"
    print(formatted)
    state["logs"].append(formatted)
    state["log_seq"] = state.get("log_seq", 0) + 1
    if len(state["logs"]) > 100:
        state["logs"].pop(0)

def save_state():
    try:
        data_to_save = {
            "paper_balance": state["paper_balance"],
            "active_trades": state["active_trades"],
            "trade_history": state["trade_history"],
            "last_trade_side": state["last_trade_side"],
            "last_withdrawal": state.get("last_withdrawal"),
            "redeem_submitted": state.get("redeem_submitted", {})
        }
        with open(STATE_PATH, "w") as f:
            json.dump(data_to_save, f, indent=2)

        cfg_file = CONFIG_PATH if os.path.exists(CONFIG_PATH) else "config.json"
        if os.path.exists(cfg_file):
            with open(cfg_file, "r") as f:
                cfg = json.load(f)
            cfg["paper_balance_usd"] = state["paper_balance"]
            with open(cfg_file, "w") as f:
                json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"Error saving state: {e}")

def load_state():
    try:
        load_telegram_subscribers()
        if os.path.exists(STATE_PATH):
            with open(STATE_PATH, "r") as f:
                loaded = json.load(f)
                state["paper_balance"] = loaded.get("paper_balance", settings.PAPER_BALANCE_USD)
                state["active_trades"] = loaded.get("active_trades", [])
                state["trade_history"] = loaded.get("trade_history", [])
                state["last_trade_side"] = loaded.get("last_trade_side")
                state["last_withdrawal"] = loaded.get("last_withdrawal")
                state["redeem_submitted"] = loaded.get("redeem_submitted", {})
                log_message(f"State loaded from {STATE_PATH}")
    except Exception as e:
        print(f"Error loading state: {e}")

# ── Telegram notifications & poller ───────────────────────────────────────────
def load_telegram_subscribers() -> List[int]:
    try:
        if os.path.exists(TELEGRAM_SUBS_PATH):
            with open(TELEGRAM_SUBS_PATH, "r") as f:
                subs = json.load(f)
                if isinstance(subs, list):
                    state["telegram_subscribers"] = [int(s) for s in subs]
                    return state["telegram_subscribers"]
    except Exception as e:
        print(f"Error loading telegram subscribers: {e}")
    return []

def save_telegram_subscribers():
    try:
        with open(TELEGRAM_SUBS_PATH, "w") as f:
            json.dump(state["telegram_subscribers"], f, indent=2)
    except Exception as e:
        print(f"Error saving telegram subscribers: {e}")

def add_telegram_subscriber(chat_id: int):
    if chat_id not in state["telegram_subscribers"]:
        state["telegram_subscribers"].append(chat_id)
        save_telegram_subscribers()

def remove_telegram_subscriber(chat_id: int):
    if chat_id in state["telegram_subscribers"]:
        state["telegram_subscribers"].remove(chat_id)
        save_telegram_subscribers()

async def send_telegram(text: str):
    if not settings.TELEGRAM_ENABLED or not settings.TELEGRAM_BOT_TOKEN:
        return
    subs = state["telegram_subscribers"]
    if not subs:
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    proxy = ws_data.get_proxy_url_for(url)
    for chat_id in subs:
        try:
            async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=5.0) as client:
                await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
        except Exception as e:
            print(f"Failed to send telegram to {chat_id}: {e}")

async def send_telegram_to(chat_id: int, text: str):
    if not settings.TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    proxy = ws_data.get_proxy_url_for(url)
    try:
        async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=5.0) as client:
            await client.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
    except Exception as e:
        print(f"Failed to send telegram to {chat_id}: {e}")

async def telegram_poller():
    offset = 0
    while True:
        if not settings.TELEGRAM_ENABLED or not settings.TELEGRAM_BOT_TOKEN:
            await asyncio.sleep(5)
            continue
        try:
            url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/getUpdates"
            proxy = ws_data.get_proxy_url_for(url)
            async with httpx.AsyncClient(proxy=proxy if proxy else None, timeout=10.0) as client:
                resp = await client.get(url, params={"offset": offset, "timeout": 5})
                if resp.status_code == 200:
                    data_updates = resp.json().get("result", [])
                    for update in data_updates:
                        offset = max(offset, update.get("update_id", 0) + 1)
                        msg = update.get("message", {})
                        chat = msg.get("chat", {})
                        chat_id = chat.get("id")
                        text = (msg.get("text") or "").strip()
                        if not chat_id or not text:
                            continue
                        
                        cmd = text.split()[0].lower()
                        if cmd == "/start":
                            add_telegram_subscriber(chat_id)
                            await send_telegram_to(chat_id, "🤖 *Subscribed to 15m Polymarket Bot alerts!* Use /status or /balance to check current status.")
                        elif cmd == "/stop":
                            remove_telegram_subscriber(chat_id)
                            await send_telegram_to(chat_id, "👋 *Unsubscribed from alerts.*")
                        elif cmd == "/status":
                            mode = state["trading_mode"].upper()
                            running = "🟢 RUNNING" if state["running"] else "🔴 STOPPED"
                            bal = state["paper_balance"]
                            active_cnt = len(state["active_trades"])
                            msg_txt = f"📊 *Bot Status*\n• Status: {running}\n• Mode: {mode}\n• Balance: `${bal:.2f}`\n• Active Trades: `{active_cnt}`"
                            await send_telegram_to(chat_id, msg_txt)
                        elif cmd == "/balance":
                            bal = state["paper_balance"]
                            funder = clob_trader.get_funder() or "N/A"
                            await send_telegram_to(chat_id, f"💰 *Current Balance*\n• Balance: `${bal:.2f}`\n• Wallet: `{funder}`")
                        elif cmd == "/help":
                            await send_telegram_to(chat_id, "ℹ️ *Commands*\n/start - Subscribe\n/stop - Unsubscribe\n/status - Bot status\n/balance - Wallet balance")
        except Exception:
            pass
        await asyncio.sleep(2)

# ── Live PnL and State WebSocket broadcast ────────────────────────────────────
async def broadcast_state():
    if not _ws_clients:
        return
    msg = json.dumps({
        "type": "state_update",
        "data": state["latest_data"],
        "logs": state["logs"][-20:],
        "log_seq": state["log_seq"],
        "redeemable": state["redeemable"],
        "redeem_run": state["redeem_run"],
    })
    dead = set()
    for ws in list(_ws_clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    for ws in dead:
        _ws_clients.discard(ws)

# ── Background task streams ───────────────────────────────────────────────────
def get_ws_symbol_filter(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return s[:-4].lower()
    return s.lower()

binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL, on_update=_wake_entry)
binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)

polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
    ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
    symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
)
polymarket_clob_ws = ws_data.PolymarketClobMarketStream()
chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))

def get_candle_window_timing(window_minutes: int) -> Dict[str, float]:
    now_ms = time.time() * 1000
    window_ms = window_minutes * 60_000
    start_ms = (now_ms // window_ms) * window_ms
    end_ms = start_ms + window_ms
    elapsed_ms = now_ms - start_ms
    remaining_ms = end_ms - now_ms
    return {
        "startMs": start_ms,
        "endMs": end_ms,
        "elapsedMs": elapsed_ms,
        "remainingMs": remaining_ms,
        "elapsedMinutes": elapsed_ms / 60_000,
        "remainingMinutes": remaining_ms / 60_000
    }

async def fetch_polymarket_snapshot() -> Dict[str, Any]:
    market = None
    if settings.POLYMARKET_SLUG:
        market = await data.fetch_market_by_slug(settings.POLYMARKET_SLUG)
    elif settings.POLYMARKET_AUTO_SELECT_LATEST:
        events = await data.fetch_live_events_by_series_id(settings.POLYMARKET_SERIES_ID)
        markets = data.flatten_event_markets(events)

        now = time.time() * 1000
        live_markets = [m for m in markets if m.get("endDate") and datetime.fromisoformat(m["endDate"].replace('Z', '+00:00')).timestamp() * 1000 > now]
        if live_markets:
            live_markets.sort(key=lambda x: x["endDate"])
            market = live_markets[0]

    if not market:
        return {"ok": False, "reason": "market_not_found"}

    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    clob_token_ids = market.get("clobTokenIds", [])
    if isinstance(clob_token_ids, str):
        clob_token_ids = json.loads(clob_token_ids)

    outcome_prices = market.get("outcomePrices", [])
    if isinstance(outcome_prices, str):
        outcome_prices = json.loads(outcome_prices)

    up_token_id = None
    down_token_id = None

    for i, outcome in enumerate(outcomes):
        token_id = clob_token_ids[i] if i < len(clob_token_ids) else None
        if not token_id: continue
        if outcome.lower() == settings.POLYMARKET_UP_LABEL.lower():
            up_token_id = token_id
        elif outcome.lower() == settings.POLYMARKET_DOWN_LABEL.lower():
            down_token_id = token_id

    up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), -1)
    down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), -1)

    gamma_yes = float(outcome_prices[up_index]) if up_index >= 0 and up_index < len(outcome_prices) else None
    gamma_no = float(outcome_prices[down_index]) if down_index >= 0 and down_index < len(outcome_prices) else None

    if not up_token_id or not down_token_id:
        return {"ok": False, "reason": "missing_token_ids"}

    # Update active tokens in the WebSocket stream
    polymarket_clob_ws.update_assets([up_token_id, down_token_id])

    up_ws = polymarket_clob_ws.get_token_market(up_token_id)
    down_ws = polymarket_clob_ws.get_token_market(down_token_id)

    # Use WS best_ask if available, else fallback to REST
    up_buy = up_ws.get("best_ask")
    down_buy = down_ws.get("best_ask")

    if up_buy is None or down_buy is None:
        try:
            up_rest_price, down_rest_price = await asyncio.gather(
                data.fetch_clob_price(up_token_id, "buy") if up_buy is None else asyncio.sleep(0, result=up_buy),
                data.fetch_clob_price(down_token_id, "buy") if down_buy is None else asyncio.sleep(0, result=down_buy)
            )
            if up_buy is None: up_buy = up_rest_price
            if down_buy is None: down_buy = down_rest_price
        except Exception:
            pass

    # Build orderbook summaries using WS orderbooks if available, fallback to REST
    up_book_summary = None
    down_book_summary = None

    if up_ws.get("bids") or up_ws.get("asks"):
        up_book_summary = data.summarize_order_book(up_ws)
    if down_ws.get("bids") or down_ws.get("asks"):
        down_book_summary = data.summarize_order_book(down_ws)

    if not up_book_summary or not down_book_summary:
        try:
            up_book, down_book = await asyncio.gather(
                data.fetch_order_book(up_token_id) if not up_book_summary else asyncio.sleep(0, result={}),
                data.fetch_order_book(down_token_id) if not down_book_summary else asyncio.sleep(0, result={})
            )
            if not up_book_summary: up_book_summary = data.summarize_order_book(up_book)
            if not down_book_summary: down_book_summary = data.summarize_order_book(down_book)
        except Exception:
            if not up_book_summary:
                up_book_summary = {"bestBid": None, "bestAsk": up_buy, "spread": None, "bidLiquidity": None, "askLiquidity": None}
            if not down_book_summary:
                down_book_summary = {"bestBid": None, "bestAsk": down_buy, "spread": None, "bidLiquidity": None, "askLiquidity": None}

    # Track whether the book data came from WS or REST
    book_source = "ws" if ((up_ws.get("bids") or up_ws.get("asks")) and (down_ws.get("bids") or down_ws.get("asks"))) else "rest"

    return {
        "ok": True,
        "market": market,
        "prices": {
            "up": up_buy if up_buy is not None else gamma_yes,
            "down": down_buy if down_buy is not None else gamma_no
        },
        "token_ids": {
            "up": up_token_id,
            "down": down_token_id
        },
        "orderbook": {
            "up": up_book_summary,
            "down": down_book_summary
        },
        "book_source": book_source
    }

async def execute_trade(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any], strike_open: Optional[float], token_ids: Dict[str, Any], orderbook: Optional[Dict[str, Any]] = None,
                        strike_source: str = "chainlink_ws", window_start_ms: Optional[int] = None,
                        open_reason: str = "ev_entry"):
    if decision["action"] != "ENTER":
        return decision.get("reason", "no_trade")

    # CONSTRAINT: Only one position per market window — do NOT block entering a new window while an old one is resolving
    cur_mkt_id = str(market.get("id"))
    if any(str(t.get("market_id")) == cur_mkt_id for t in state["active_trades"]):
        return "slot_busy"

    if state["withdraw_state"] == "in_progress":
        return "withdraw_in_progress"

    if strike_open is None:
        return "no_strike"

    side = decision["side"]
    price = market_prices["up"] if side == "UP" else market_prices["down"]
    if price is None:
        return "no_price"

    balance = state["paper_balance"]
    risk_type = (settings.RISK_TYPE or "percent").lower()
    if risk_type == "fixed":
        amount_to_risk = float(settings.RISK_VALUE)
    else:
        amount_to_risk = (float(settings.RISK_VALUE) / 100.0) * balance

    if amount_to_risk <= 0:
        return "stake_zero"

    ob = (orderbook or {}).get("up" if side == "UP" else "down") or {}
    ask_liq_shares = ob.get("askLiquidity")
    if ask_liq_shares is not None and price > 0:
        ask_liq_usd = ask_liq_shares * price
        if ask_liq_usd < settings.MIN_BOOK_LIQUIDITY_USD:
            log_message(f"Skip {side}: thin book (${ask_liq_usd:.2f} ask liquidity)")
            return "thin_book"
        amount_to_risk = min(amount_to_risk, ask_liq_usd)

    if balance < amount_to_risk or amount_to_risk <= 0:
        print(f"Insufficient balance ({balance}) or invalid risk amount ({amount_to_risk})")
        return "insufficient_balance"

    end_date_str = market.get("endDate")
    end_ts = 0
    if end_date_str:
        try:
            end_ts = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')).timestamp()
        except Exception:
            pass
    if not end_ts:
        end_ts = time.time() + settings.CANDLE_WINDOW_MINUTES * 60

    trade = {
        "market_id": market["id"],
        "market_slug": market.get("slug"),
        "side": side,
        "entry_price": price,
        "amount": amount_to_risk,
        "shares": amount_to_risk / price,
        "entry_time": datetime.now().isoformat(),
        "status": "OPEN",
        "settlement_price": None,
        "profit_loss": None,
        "strike_price": strike_open,
        "strike_source": strike_source,
        "window_start_ms": int(window_start_ms) if window_start_ms is not None else None,
        "open_reason": open_reason,
        "close_price": None,
        "end_ts": end_ts,
        "mode": state["trading_mode"]
    }

    if state["trading_mode"] == "paper":
        state["paper_balance"] -= amount_to_risk
        state["active_trades"].append(trade)
        state["last_trade_side"] = side
        save_state()

        msg = f"Executed PAPER trade: {side} @ {price:.4f} for {market.get('slug')} (Amount: ${amount_to_risk:.2f})"
        log_message(msg)
        await send_telegram(f"🟢 *PAPER Trade Entered*\n• Side: `{side}`\n• Price: `{price:.4f}`\n• Stake: `${amount_to_risk:.2f}`\n• Market: `{market.get('slug')}`")
        return "entered"
    else:
        token_id = token_ids.get("up") if side == "UP" else token_ids.get("down")
        if not token_id:
            log_message(f"LIVE trade aborted: missing token_id for side {side}")
            return "missing_token_id"

        result = await asyncio.to_thread(clob_trader.place_market_buy, token_id, amount_to_risk, price)
        if result.get("ok"):
            trade["order_id"] = result.get("order_id")
            trade["order_response"] = result.get("response") or {}
            trade["token_id"] = token_id
            fill_size = result.get("fill_size")
            fill_price = result.get("fill_price")
            fill_usd = result.get("fill_usd")
            if fill_size and fill_price:
                trade["shares"] = float(fill_size)
                trade["entry_price"] = float(fill_price)
                trade["amount"] = float(fill_usd if fill_usd else fill_size * fill_price)
                trade["quoted_price"] = price
                trade["slippage"] = float(fill_price) - float(price) if price else None
            state["active_trades"].append(trade)
            state["last_trade_side"] = side
            save_state()
            msg = (f"Executed LIVE trade [FAK]: {side} ${trade['amount']:.2f} on {market.get('slug')} "
                   f"— {trade['shares']:.2f} shares @ {trade['entry_price']:.4f} (quote {price}, order {trade['order_id']})")
            log_message(msg)
            await send_telegram(f"🚀 *LIVE Trade Entered [FAK]*\n• Side: `{side}`\n• Price: `{trade['entry_price']:.4f}`\n• Shares: `{trade['shares']:.2f}`\n• Amount: `${trade['amount']:.2f}`\n• Market: `{market.get('slug')}`")
            return "entered"
        else:
            log_message(f"LIVE trade FAILED ({side}): {result.get('error')}")
            return "live_order_failed"

async def maybe_flip_position(decision: Dict[str, Any], poly_snapshot: Dict[str, Any], time_left_min: Optional[float]):
    if not settings.FLIP_ENABLED:
        return
    if decision.get("action") != "ENTER" or not state["active_trades"]:
        return

    new_side = decision["side"]
    new_prob = decision.get("prob", 0) or 0
    if new_prob < settings.FLIP_MIN_CONVICTION:
        return
    if time_left_min is not None and time_left_min < settings.FLIP_MIN_MINUTES_LEFT:
        return

    market = poly_snapshot["market"]
    prices = poly_snapshot["prices"]
    token_ids = poly_snapshot.get("token_ids", {})
    orderbook = poly_snapshot.get("orderbook", {})

    cur_trades = [t for t in state["active_trades"] if str(t.get("market_id")) == str(market.get("id"))]
    if not cur_trades:
        return
    trade = cur_trades[0]
    if trade["side"] == new_side:
        return

    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    exit_price = ob.get("bestBid") or prices.get(held_key)
    if not exit_price or exit_price <= 0:
        log_message(f"FLIP aborted: no exit price for {trade['side']}")
        return

    if state["trading_mode"] == "live":
        token_id = token_ids.get(held_key)
        result = await asyncio.to_thread(clob_trader.place_market_sell, token_id, trade["shares"], exit_price)
        if not result.get("ok"):
            log_message(f"FLIP sell FAILED ({trade['side']}): {result.get('error')} — position kept")
            return
        if result.get("fill_price"):
            exit_price = float(result["fill_price"])
        trade["exit_order_id"] = result.get("order_id")
    else:
        state["paper_balance"] += trade["shares"] * exit_price

    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = "flip"
    trade["resolution"] = "flip_exit"
    trade["settlement_price_at_expiry"] = exit_price
    trade["open_price"] = trade.get("strike_price")
    trade["close_price"] = state.get("last_seen_price")
    trade["profit_loss"] = (trade["shares"] * exit_price) - trade["amount"]
    state["trade_history"].append(_archive(trade))
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    save_state()
    log_message(f"FLIP: closed {trade['side']} @ {exit_price:.2f} (P/L ${trade['profit_loss']:.2f}); opening {new_side}")
    return new_side

def mark_window_open(start_ms: int, window_ms: int, current_price: Optional[float],
                      spot_price: Optional[float], price_source: Optional[str],
                      price_is_fresh: bool = True) -> Dict[str, Any]:
    opens = state["market_opens"]
    prev_ws = state.get("last_window_start")

    # On rollover, freeze the PRIOR window's close
    if prev_ws is not None and prev_ws != start_ms and prev_ws in opens:
        if opens[prev_ws].get("close") is None and state.get("last_seen_price"):
            opens[prev_ws]["close"] = state["last_seen_price"]
    if current_price:
        state["last_seen_price"] = current_price

    observed_prev = prev_ws is not None and abs((start_ms - window_ms) - prev_ws) < 2000
    if start_ms not in opens:
        opens[start_ms] = {"chainlink": None, "binance": None,
                           "close": None, "genuine": observed_prev}
        for k in list(opens.keys()):
            if k < start_ms - 4 * window_ms:
                del opens[k]

    win = opens[start_ms]
    since_start = time.time() * 1000 - start_ms
    if (win["chainlink"] is None and current_price and price_is_fresh
            and 0 <= since_start < MARK_CAPTURE_WINDOW_MS):
        win["chainlink"] = current_price
        win["binance"] = spot_price
        win["genuine"] = True
        log_message(f"Window open marked @ eventStartTime: Chainlink {current_price:.2f} "
                    f"({price_source}) / Binance {spot_price if spot_price else '-'}")
    state["last_window_start"] = start_ms
    return win

async def _redeem_win(trade: Dict[str, Any], market: Optional[Dict[str, Any]],
                      up_index: int, down_index: int, winning_index: int):
    condition_id = (market or {}).get("conditionId") or (market or {}).get("condition_id")
    if not condition_id:
        trade["redeem"] = {"ok": False, "error": "missing_condition_id"}
        log_message(f"REDEEM skipped for {trade['market_slug']}: no conditionId on the market")
        return

    amounts = [0.0, 0.0]
    idx = up_index if winning_index == up_index else down_index
    if 0 <= idx < len(amounts):
        amounts[idx] = float(trade.get("shares") or 0.0)

    neg_risk = bool((market or {}).get("negRisk") or (market or {}).get("neg_risk") or False)
    try:
        res = await asyncio.to_thread(clob_trader.redeem, condition_id, amounts, neg_risk)
    except Exception as e:
        res = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    trade["redeem"] = res
    if res.get("ok"):
        state["redeem_submitted"][condition_id] = time.time()
        log_message(f"REDEEM ok for {trade['market_slug']}: {amounts[idx]:.2f} shares (tx {res.get('tx')})")
        _redeem_wake.set()
    else:
        log_message(f"REDEEM FAILED for {trade['market_slug']}: {res.get('error')} "
                    f"— redeem manually via dashboard to free the capital")

def _archive(trade: Dict[str, Any]) -> Dict[str, Any]:
    for k in ("_market", "_market_closed", "order_response"):
        trade.pop(k, None)
    return trade

async def update_trades(current_prices: Dict[str, Any]):
    # Snapshot pattern to prevent mid-pass mutations from dropping concurrent trades
    active_snapshot = list(state["active_trades"])
    remaining_active = []
    trades_changed = False
    now_ts = time.time()

    cur_price = current_prices.get("chainlink") or current_prices.get("spot")
    SETTLEMENT_GRACE_SECONDS = 300

    for trade in active_snapshot:
        if cur_price:
            trade["last_price"] = cur_price

        end_ts = trade.get("end_ts", 0)
        if not end_ts:
            try:
                end_ts = datetime.fromisoformat(trade["entry_time"]).timestamp() + settings.CANDLE_WINDOW_MINUTES * 60
            except Exception:
                end_ts = now_ts
        expired = now_ts >= end_ts

        if expired and trade.get("close_price") is None:
            frozen_close = cur_price or trade.get("last_price")
            if frozen_close:
                trade["close_price"] = frozen_close

        market = trade.get("_market")
        poll_every = 3.0 if expired else 30.0
        if trade.get("last_api_check", 0) < now_ts - poll_every:
            try:
                fetched = await data.fetch_market_by_slug(trade["market_slug"])
            except Exception:
                fetched = None
            trade["last_api_check"] = now_ts
            if fetched is not None:
                market = fetched
                trade["_market"] = fetched
                trade["_market_closed"] = bool(fetched.get("closed"))
        market_closed = trade.get("_market_closed", False)

        if not expired and not market_closed:
            remaining_active.append(trade)
            continue

        outcomes = []
        outcome_prices = []
        if market:
            outcomes = market.get("outcomes", [])
            if isinstance(outcomes, str): outcomes = json.loads(outcomes)
            outcome_prices = market.get("outcomePrices", [])
            if isinstance(outcome_prices, str): outcome_prices = json.loads(outcome_prices)
        if not outcomes:
            outcomes = [settings.POLYMARKET_UP_LABEL, settings.POLYMARKET_DOWN_LABEL]

        up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), 0)
        down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), 1)

        winning_index = -1
        resolution = None
        for i, p in enumerate(outcome_prices):
            try:
                if float(p) > 0.9:
                    winning_index = i
                    resolution = "polymarket_settled"
                    break
            except Exception:
                pass

        strike = trade.get("strike_price")
        settlement_price = (trade.get("close_price") or trade.get("settlement_price_at_expiry")
                            or trade.get("last_price") or cur_price)
        if winning_index == -1 and (expired or market_closed):
            if trade.get("expired_at") is None:
                trade["expired_at"] = now_ts
            waited = now_ts - trade["expired_at"]
            if waited < settings.AUTHORITATIVE_SETTLE_WAIT_S and not market_closed:
                remaining_active.append(trade)
                continue
            if strike and settlement_price:
                trade["settlement_price_at_expiry"] = settlement_price
                winning_index = up_index if settlement_price > strike else down_index
                resolution = "close_vs_open"
                trade["settle_wait_s"] = round(waited, 1)

        if winning_index == -1:
            first_seen = trade.get("unresolved_since")
            if first_seen is None:
                trade["unresolved_since"] = now_ts
                remaining_active.append(trade)
                continue
            if now_ts - first_seen < SETTLEMENT_GRACE_SECONDS:
                remaining_active.append(trade)
                continue
            trade["status"] = "VOID"
            trade["exit_reason"] = "void"
            trade["exit_time"] = datetime.now().isoformat()
            trade["profit_loss"] = 0.0
            if trade.get("mode", "paper") == "paper":
                state["paper_balance"] += trade["amount"]
            state["trade_history"].append(_archive(trade))
            trades_changed = True
            log_message(f"VOID: Trade for {trade['market_slug']} unresolved past grace; stake refunded (paper).")
            continue

        won = ((trade["side"] == "UP" and winning_index == up_index) or
               (trade["side"] == "DOWN" and winning_index == down_index))

        open_px = strike
        close_px = trade.get("close_price") or settlement_price
        trade["open_price"] = open_px
        trade["close_price"] = close_px
        trade["resolution"] = resolution or "unknown"
        if open_px and close_px:
            move_side = "UP" if close_px > open_px else "DOWN"
            dir_txt = f"open {open_px:.2f} -> close {close_px:.2f} ({move_side} by {abs(close_px - open_px):.2f})"
        else:
            dir_txt = f"open {open_px} -> close {close_px}"

        if won:
            payout = trade["shares"] * 1.0
            if trade.get("mode", "paper") == "paper":
                state["paper_balance"] += payout
            trade["profit_loss"] = payout - trade["amount"]
            log_message(f"WIN: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                        f"[{trade['resolution']}]. Profit: ${trade['profit_loss']:.2f}")
            await send_telegram(f"🏆 *WIN: {trade['side']}*\n• Profit: `+${trade['profit_loss']:.2f}`\n• Details: {dir_txt}\n• Market: `{trade['market_slug']}`")
            if trade.get("mode") == "live":
                await _redeem_win(trade, market, up_index, down_index, winning_index)
        else:
            trade["profit_loss"] = -trade["amount"]
            log_message(f"LOSS: {trade['side']} on {trade['market_slug']}: {dir_txt} "
                        f"[{trade['resolution']}]. Loss: ${trade['profit_loss']:.2f}")
            await send_telegram(f"❌ *LOSS: {trade['side']}*\n• Loss: `-${trade['amount']:.2f}`\n• Details: {dir_txt}\n• Market: `{trade['market_slug']}`")

        trade["status"] = "CLOSED"
        trade["exit_reason"] = trade.get("exit_reason") or "settled"
        trade["exit_time"] = datetime.now().isoformat()
        trade["settlement_price_at_expiry"] = trade.get("settlement_price_at_expiry") or settlement_price
        trade["winning_outcome"] = outcomes[winning_index] if 0 <= winning_index < len(outcomes) else None
        state["trade_history"].append(_archive(trade))
        trades_changed = True

    state["active_trades"] = remaining_active
    if trades_changed:
        save_state()

# ── Capital Extractor (Auto-Withdrawal State Machine) ─────────────────────────
async def maybe_auto_withdraw(equity: float, poly_snapshot: Dict[str, Any]):
    if not settings.AUTO_WITHDRAW_ENABLED:
        state["withdraw_state"] = "idle"
        return
    if state["trading_mode"] != "live":
        return
    if not settings.WITHDRAW_ADDRESS or settings.WITHDRAW_AMOUNT <= 0:
        return

    now_ts = time.time()
    st = state["withdraw_state"]

    if st == "idle":
        if equity >= settings.WITHDRAW_TRIGGER_BALANCE:
            if not state["active_trades"]:
                log_message(f"CAPITAL EXTRACTOR: Trigger reached (${equity:.2f} >= ${settings.WITHDRAW_TRIGGER_BALANCE:.2f}). Initiating withdrawal of ${settings.WITHDRAW_AMOUNT:.2f}...")
                state["withdraw_state"] = "in_progress"
                state["withdraw_submitted_at"] = now_ts
                state["withdraw_locked_market"] = poly_snapshot.get("market", {}).get("id") if poly_snapshot.get("ok") else None
                
                res = await asyncio.to_thread(clob_trader.withdraw_pusd, settings.WITHDRAW_ADDRESS, settings.WITHDRAW_AMOUNT)
                if res.get("ok"):
                    tx = res.get("tx")
                    state["last_withdrawal"] = {
                        "amount": settings.WITHDRAW_AMOUNT,
                        "recipient": settings.WITHDRAW_ADDRESS,
                        "timestamp": datetime.now().isoformat(),
                        "tx": tx,
                        "status": "submitted"
                    }
                    save_state()
                    log_message(f"CAPITAL EXTRACTOR: Withdrawal tx submitted ({tx}). Waiting confirmation...")
                    await send_telegram(f"💸 *Capital Extractor*\nWithdrew `${settings.WITHDRAW_AMOUNT:.2f}` pUSD to `{settings.WITHDRAW_ADDRESS}`\nTx: `{tx}`")
                else:
                    log_message(f"CAPITAL EXTRACTOR FAILED: {res.get('error')}")
                    state["withdraw_state"] = "cooldown"
                    state["withdraw_submitted_at"] = now_ts

    elif st == "in_progress":
        last_w = state.get("last_withdrawal")
        tx = (last_w or {}).get("tx")
        confirmed = False
        if tx:
            confirmed = await asyncio.to_thread(clob_trader.is_tx_confirmed, tx)
        if confirmed or (now_ts - (state["withdraw_submitted_at"] or now_ts) > 45):
            if last_w:
                last_w["status"] = "confirmed"
                save_state()
            log_message(f"CAPITAL EXTRACTOR: Withdrawal confirmed! Entering cooldown.")
            state["withdraw_state"] = "cooldown"
            state["withdraw_submitted_at"] = now_ts

    elif st == "cooldown":
        resume_after = (settings.WITHDRAW_RESUME_AFTER or "flat").lower()
        if not settings.WITHDRAW_AUTO_RESUME:
            return

        can_resume = False
        sub_at = state.get("withdraw_submitted_at") or now_ts
        if resume_after == "flat":
            if now_ts - sub_at > 30:
                can_resume = True
        elif resume_after == "next_window":
            cur_mkt_id = poly_snapshot.get("market", {}).get("id") if poly_snapshot.get("ok") else None
            if cur_mkt_id and cur_mkt_id != state.get("withdraw_locked_market"):
                can_resume = True
        else:
            if now_ts - sub_at > 30:
                can_resume = True

        if can_resume:
            log_message("CAPITAL EXTRACTOR: Resuming trading operations.")
            state["withdraw_state"] = "idle"
            state["withdraw_locked_market"] = None

# ── Event-driven early evaluation & watcher ───────────────────────────────────
def entry_block_reason(ctx: Dict[str, Any]) -> Optional[str]:
    if not state["running"]:
        return "stopped"
    if state["withdraw_state"] == "in_progress":
        return "withdraw_in_progress"
    if not ctx:
        return "no_context"
    if ctx.get("slot_busy"):
        return "slot_busy"
    if not ctx.get("price_is_fresh"):
        return "stale_feed"
    if ctx.get("strike_open") is None:
        return "no_strike"
    if not ctx.get("market_ok"):
        return "market_not_ready"
    return None

async def evaluate_entry(reason: str = "event"):
    global _last_eval_ts
    if _entry_lock.locked():
        return
    async with _entry_lock:
        now = time.time()
        if now - _last_eval_ts < MIN_EVAL_INTERVAL_S:
            return
        _last_eval_ts = now

        ctx = state.get("trade_ctx")
        if not ctx:
            return
        if now - ctx.get("built_at", 0) > CTX_MAX_AGE_S:
            return

        block = entry_block_reason(ctx)
        if block:
            return

        spot_price = binance_stream.get_last().get("price")
        if not spot_price:
            return

        mc_steps = max(1, math.ceil(ctx["time_left_min"] / 5))
        fair_up = indicators.fair_prob_up(
            spot_price, ctx["target_open"], mc_steps, ctx["sigma_5m"], drift_per_step=ctx["drift_5m"]
        )

        market_up = ctx["prices"]["up"]
        market_down = ctx["prices"]["down"]

        up_ws_sum = polymarket_clob_ws.get_summary(ctx["token_ids"].get("up"), max_age_s=settings.MAX_BOOK_AGE_S)
        down_ws_sum = polymarket_clob_ws.get_summary(ctx["token_ids"].get("down"), max_age_s=settings.MAX_BOOK_AGE_S)
        if up_ws_sum and up_ws_sum.get("bestAsk"):
            market_up = up_ws_sum["bestAsk"]
        if down_ws_sum and down_ws_sum.get("bestAsk"):
            market_down = down_ws_sum["bestAsk"]

        decision = engines.decide_ev({
            "mcProbUp": fair_up,
            "priceUp": market_up,
            "priceDown": market_down,
            "minProb": settings.MIN_PROB_EV,
            "evThreshold": settings.EV_THRESHOLD,
            "rsi": ctx["rsi"],
            "haExhaustedGreen": ctx["ha_exhausted_green"],
            "haExhaustedRed": ctx["ha_exhausted_red"],
        })

        if decision.get("action") == "ENTER":
            market_prices = {"up": market_up, "down": market_down}
            exec_res = await execute_trade(
                decision, market_prices, ctx["market"], ctx["strike_open"],
                ctx["token_ids"], ctx.get("orderbook", {}),
                strike_source=ctx["strike_source"], window_start_ms=ctx["start_ms"],
                open_reason=f"event_entry_{reason}"
            )
            state["event_exec"] = {
                "ts": datetime.now().isoformat(),
                "side": decision["side"],
                "reason": reason,
                "result": exec_res
            }
            if exec_res == "entered":
                await broadcast_state()

async def entry_watcher():
    while True:
        try:
            await _market_event.wait()
            _market_event.clear()
            await evaluate_entry(reason="trade_tick")
        except Exception:
            await asyncio.sleep(0.1)

# ── Manual & background redemption ────────────────────────────────────────────
async def refresh_redeemable():
    if state["trading_mode"] != "live":
        state["redeemable"] = {"positions": [], "count": 0, "value": 0.0, "checked_at": datetime.now().isoformat(), "error": None}
        return state["redeemable"]
    funder = clob_trader.get_funder()
    if not funder:
        state["redeemable"] = {"positions": [], "count": 0, "value": 0.0, "checked_at": datetime.now().isoformat(), "error": "no_funder"}
        return state["redeemable"]
    try:
        raw_pos = await data.fetch_redeemable_positions(funder)
        now_ts = time.time()
        valid_pos = []
        tot_val = 0.0
        for p in raw_pos:
            cid = p.get("conditionId") or p.get("condition_id")
            size = float(p.get("size") or 0.0)
            cur_val = float(p.get("currentValue") or (float(p.get("curPrice", 0) or 0) * size) or 0.0)
            resolved = p.get("resolved") or p.get("redeemable") or (p.get("curPrice") == 1.0)
            if size > 0 and (resolved or p.get("redeemable")):
                sub_ts = state["redeem_submitted"].get(cid, 0)
                if now_ts - sub_ts > REDEEM_SUBMITTED_HIDE_S:
                    valid_pos.append(p)
                    tot_val += cur_val if cur_val > 0 else size
        state["redeemable"] = {
            "positions": valid_pos,
            "count": len(valid_pos),
            "value": round(tot_val, 2),
            "checked_at": datetime.now().isoformat(),
            "error": None
        }
    except Exception as e:
        state["redeemable"] = {"positions": [], "count": 0, "value": 0.0, "checked_at": datetime.now().isoformat(), "error": str(e)}
    return state["redeemable"]

async def _redeem_all_run():
    async with _redeem_lock:
        if state["redeem_run"]["busy"]:
            return state["redeem_run"]
        state["redeem_run"]["busy"] = True
        state["redeem_run"]["errors"] = []
        try:
            await refresh_redeemable()
            positions = state["redeemable"].get("positions", [])
            state["redeem_run"]["total"] = len(positions)
            state["redeem_run"]["done"] = 0
            state["redeem_run"]["ok"] = 0
            state["redeem_run"]["failed"] = 0
            state["redeem_run"]["value"] = state["redeemable"].get("value", 0.0)

            for p in positions:
                cid = p.get("conditionId") or p.get("condition_id")
                size = float(p.get("size") or 0.0)
                neg_risk = bool(p.get("negRisk") or p.get("neg_risk") or False)
                outcome_idx = int(p.get("outcomeIndex", 0))
                amts = [0.0, 0.0]
                if 0 <= outcome_idx < len(amts):
                    amts[outcome_idx] = size
                else:
                    amts = [size, 0.0]
                
                res = await asyncio.to_thread(clob_trader.redeem, cid, amts, neg_risk)
                state["redeem_run"]["done"] += 1
                if res.get("ok"):
                    state["redeem_run"]["ok"] += 1
                    state["redeem_submitted"][cid] = time.time()
                    log_message(f"MANUAL REDEEM: Successfully redeemed {size:.2f} shares for condition {cid[:8]}... (tx: {res.get('tx')})")
                else:
                    state["redeem_run"]["failed"] += 1
                    state["redeem_run"]["errors"].append(f"{cid[:8]}: {res.get('error')}")
                    log_message(f"MANUAL REDEEM FAILED: {res.get('error')} on condition {cid[:8]}...")
            
            state["redeem_run"]["finished_at"] = datetime.now().isoformat()
            save_state()
            await refresh_redeemable()
            await broadcast_state()
        finally:
            state["redeem_run"]["busy"] = False
        return state["redeem_run"]

async def redeem_watcher():
    while True:
        try:
            await asyncio.sleep(REDEEM_CHECK_INTERVAL_S)
            await refresh_redeemable()
            await broadcast_state()
        except Exception:
            pass

async def seed_kline_buffers():
    try:
        k1m, k5m = await asyncio.gather(
            data.fetch_klines(settings.SYMBOL, "1m", 240),
            data.fetch_klines(settings.SYMBOL, "5m", 200)
        )
        binance_kline_1m.set_candles(k1m)
        binance_kline_5m.set_candles(k5m)
        log_message(f"Seeded Binance kline buffers (1m/5m) for {settings.SYMBOL}")
    except Exception as e:
        log_message(f"Failed to seed kline buffers: {e}")

# ── Main update loop ──────────────────────────────────────────────────────────
async def update_loop():
    csv_header = [
        "timestamp", "entry_minute", "time_left_min", "signal",
        "model_up", "model_down", "mkt_up", "mkt_down", "edge_up", "edge_down",
        "recommendation", "reason", "exec_result"
    ]

    while True:
        try:
            timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)

            binance_ws = binance_stream.get_last()
            if not binance_ws.get("price"):
                poly_ws_last = polymarket_ws_stream.get_last()
                cl_ws_last = chainlink_ws_stream.get_last()
                binance_ws["price"] = poly_ws_last.get("price") or cl_ws_last.get("price")
            poly_ws = polymarket_ws_stream.get_last()
            cl_ws = chainlink_ws_stream.get_last()

            results = await asyncio.gather(
                data.fetch_last_price(settings.SYMBOL),
                chainlink.chainlink_fetcher.fetch_chainlink_btc_usd(),
                fetch_polymarket_snapshot(),
                return_exceptions=True
            )

            last_price = results[0] if not isinstance(results[0], Exception) else None
            chainlink_data = results[1] if not isinstance(results[1], Exception) else {}
            poly_snapshot = results[2] if not isinstance(results[2], Exception) else {"ok": False}

            klines_1m = binance_kline_1m.get_candles()
            klines_5m = binance_kline_5m.get_candles()

            spot_price = binance_ws.get("price") if binance_ws and binance_ws.get("price") else last_price

            mc_steps = max(1, math.ceil(timing["remainingMinutes"] / 5))

            # Settle feed & freshness
            current_price = None
            price_source = None
            price_is_fresh = True
            poly_updated = poly_ws.get("updatedAt") or 0
            if poly_ws.get("price"):
                current_price = poly_ws["price"]
                price_source = "Polymarket WS"
                if poly_updated > 0:
                    age_ms = (time.time() * 1000) - poly_updated
                    if age_ms > POLY_WS_MAX_AGE_MS:
                        price_is_fresh = False
            elif cl_ws.get("price"):
                current_price = cl_ws["price"]
                price_source = "Chainlink RPC WS"
            elif chainlink_data.get("price"):
                current_price = chainlink_data["price"]
                price_source = "Chainlink RPC REST"

            window_ms = settings.CANDLE_WINDOW_MINUTES * 60_000
            event_start_ms = None
            if poly_snapshot.get("ok"):
                _mkt = poly_snapshot["market"]
                _esr = _mkt.get("eventStartTime") or _mkt.get("gameStartTime")
                if _esr:
                    try:
                        event_start_ms = int(datetime.fromisoformat(str(_esr).replace('Z', '+00:00')).timestamp() * 1000)
                    except Exception:
                        event_start_ms = None
                if event_start_ms is None and _mkt.get("endDate"):
                    try:
                        event_start_ms = int(datetime.fromisoformat(_mkt["endDate"].replace('Z', '+00:00')).timestamp() * 1000) - window_ms
                    except Exception:
                        event_start_ms = None
            if event_start_ms is None:
                event_start_ms = int(timing["startMs"])

            start_ms = event_start_ms
            win = mark_window_open(start_ms, window_ms, current_price, spot_price, price_source, price_is_fresh=price_is_fresh)

            strike_open = win["chainlink"]
            strike_source = "chainlink_ws"

            model_open = None
            for c in reversed(klines_5m):
                if c["openTime"] == start_ms:
                    model_open = c["open"]
                    break
                if c["openTime"] < start_ms:
                    break
            model_open = model_open or win.get("binance")
            target_open = model_open if strike_open is not None else None

            drift_5m, sigma_5m = indicators.realized_drift_vol(klines_5m, lookback=300)
            fair_up = indicators.fair_prob_up(spot_price or 0, target_open or 0, mc_steps, sigma_5m, drift_per_step=drift_5m or 0.0)
            fair_data = {
                "prob_up": fair_up,
                "prob_down": 1.0 - fair_up,
                "bias": "BULLISH" if fair_up > 0.6 else "BEARISH" if fair_up < 0.4 else "NEUTRAL",
                "steps": mc_steps,
                "sigma_5m": sigma_5m,
            }

            settlement_ms = None
            if poly_snapshot["ok"] and poly_snapshot["market"].get("endDate"):
                settlement_ms = datetime.fromisoformat(poly_snapshot["market"]["endDate"].replace('Z', '+00:00')).timestamp() * 1000

            time_left_min = (settlement_ms - time.time() * 1000) / 60_000 if settlement_ms else timing["remainingMinutes"]

            closes = [c["close"] for c in klines_1m]
            rsi_now = indicators.compute_rsi(closes, settings.RSI_PERIOD)

            consec = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_1m))
            consec_5m = {"color": None, "count": 0}
            if len(klines_5m) >= 20:
                consec_5m = indicators.count_consecutive(indicators.compute_heiken_ashi(klines_5m))

            market_up = poly_snapshot["prices"]["up"] if poly_snapshot["ok"] else None
            market_down = poly_snapshot["prices"]["down"] if poly_snapshot["ok"] else None

            market_implied_up = None
            if market_up is not None and market_down is not None and (market_up + market_down) > 0:
                market_implied_up = market_up / (market_up + market_down)
            edge = {
                "marketUp": market_implied_up,
                "marketDown": (1 - market_implied_up) if market_implied_up is not None else None,
                "edgeUp": (fair_up - market_implied_up) if market_implied_up is not None else None,
                "edgeDown": ((1 - fair_up) - (1 - market_implied_up)) if market_implied_up is not None else None,
            }
            prob_view = {"adjustedUp": fair_up, "adjustedDown": 1 - fair_up}

            EB = engines.EXHAUSTION_BARS
            def _is(color, count, want):
                return color == want and (count or 0) >= EB

            ha_exhausted_green = _is(consec["color"], consec["count"], "green") or _is(consec_5m["color"], consec_5m["count"], "green")
            ha_exhausted_red = _is(consec["color"], consec["count"], "red") or _is(consec_5m["color"], consec_5m["count"], "red")

            decision = engines.decide_ev({
                "mcProbUp": fair_up,
                "priceUp": market_up,
                "priceDown": market_down,
                "minProb": settings.MIN_PROB_EV,
                "evThreshold": settings.EV_THRESHOLD,
                "rsi": rsi_now,
                "haExhaustedGreen": ha_exhausted_green,
                "haExhaustedRed": ha_exhausted_red,
            })

            # Publish trade_ctx for event watcher early entries
            if poly_snapshot.get("ok"):
                state["trade_ctx"] = {
                    "built_at": time.time(),
                    "start_ms": start_ms,
                    "target_open": target_open,
                    "strike_open": strike_open,
                    "strike_source": strike_source,
                    "time_left_min": time_left_min,
                    "sigma_5m": sigma_5m,
                    "drift_5m": drift_5m or 0.0,
                    "rsi": rsi_now,
                    "ha_exhausted_green": ha_exhausted_green,
                    "ha_exhausted_red": ha_exhausted_red,
                    "prices": poly_snapshot["prices"],
                    "token_ids": poly_snapshot["token_ids"],
                    "orderbook": poly_snapshot.get("orderbook", {}),
                    "market": poly_snapshot["market"],
                    "market_ok": True,
                    "price_is_fresh": price_is_fresh,
                    "slot_busy": bool(state["active_trades"]),
                }

            current_prices_dict = {"spot": spot_price, "chainlink": current_price}

            exec_result = None
            if poly_snapshot["ok"] and state["running"] and state["withdraw_state"] != "in_progress":
                flipped = await maybe_flip_position(decision, poly_snapshot, time_left_min)
                exec_result = await execute_trade(
                    decision, poly_snapshot["prices"], poly_snapshot["market"], strike_open,
                    poly_snapshot.get("token_ids", {}), poly_snapshot.get("orderbook", {}),
                    strike_source=strike_source, window_start_ms=start_ms,
                    open_reason="flip_entry" if flipped else "ev_entry")
            elif not state["running"]:
                exec_result = "stopped"
            elif state["withdraw_state"] == "in_progress":
                exec_result = "withdraw_in_progress"

            # Check if event executor fired
            if state.get("event_exec"):
                ee = state.pop("event_exec")
                if not exec_result or exec_result in ("no_trade", "stopped"):
                    exec_result = f"event({ee.get('reason')}:{ee.get('result')})"

            await update_trades(current_prices_dict)

            # Mark open positions
            open_value = 0.0
            for t in state["active_trades"]:
                mark = None
                if poly_snapshot["ok"] and str(t.get("market_id")) == str(poly_snapshot["market"].get("id")):
                    ob = (poly_snapshot.get("orderbook") or {}).get("up" if t["side"] == "UP" else "down") or {}
                    mark = ob.get("bestBid") or (market_up if t["side"] == "UP" else market_down)
                if mark:
                    t["mark_price"] = mark
                    t["unrealized_pl"] = (t["shares"] * mark) - t["amount"]
                    open_value += t["shares"] * mark
                else:
                    t["unrealized_pl"] = None
                    open_value += t["amount"]

            # Live balance refresh
            if state["trading_mode"] == "live":
                now_ts = time.time()
                if now_ts - state.get("last_balance_refresh", 0) > 30:
                    real_bal = await asyncio.to_thread(clob_trader.get_usdc_balance)
                    if real_bal is not None:
                        state["paper_balance"] = real_bal
                    state["last_balance_refresh"] = now_ts

            total_equity = state["paper_balance"] + open_value
            await maybe_auto_withdraw(total_equity, poly_snapshot)

            signal_label = f"BUY {decision['side']}" if decision["action"] == "ENTER" else "NO TRADE"
            utils.append_csv_row(SIGNALS_PATH, csv_header, [
                datetime.now().isoformat(), timing["elapsedMinutes"], time_left_min,
                signal_label, fair_up, 1 - fair_up, market_up, market_down,
                edge["edgeUp"], edge["edgeDown"], f"{decision['side']}:{decision['phase']}:{decision['strength']}" if decision["action"] == "ENTER" else "NO_TRADE",
                decision.get("reason", ""), exec_result or ""
            ])

            state["latest_data"] = {
                "timestamp": datetime.now().isoformat(),
                "timing": timing,
                "market": poly_snapshot.get("market") if poly_snapshot["ok"] else None,
                "trading_state": {
                    "mode": state["trading_mode"],
                    "running": state["running"],
                    "balance": state["paper_balance"],
                    "equity": total_equity,
                    "open_value": open_value,
                    "active_trades": state["active_trades"],
                    "history_count": len(state["trade_history"]),
                    "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
                    "symbol": settings.SYMBOL,
                    "withdraw_state": state["withdraw_state"],
                    "last_withdrawal": state.get("last_withdrawal")
                },
                "prices": {
                    "spot": spot_price,
                    "chainlink": current_price,
                    "chainlink_source": price_source,
                    "poly_up": market_up,
                    "poly_down": market_down,
                    "window_open": strike_open,
                    "window_open_source": strike_source,
                    "model_open": model_open,
                    "window_start_ms": start_ms,
                    "price_is_fresh": price_is_fresh,
                    "book_source": poly_snapshot.get("book_source") if poly_snapshot.get("ok") else None
                },
                "indicators": {
                    "rsi": rsi_now,
                    "heiken": consec,
                    "heiken_5m": consec_5m,
                    "fair": fair_data
                },
                "analysis": {
                    "probability": prob_view, "edge": edge, "decision": decision
                }
            }
            state["last_update_ts"] = time.time()

            await broadcast_state()

        except Exception as e:
            print(f"Error in update loop: {e}")

        await asyncio.sleep(settings.POLL_INTERVAL_MS / 1000)

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()
    await seed_kline_buffers()
    if state["trading_mode"] == "live":
        asyncio.create_task(refresh_redeemable())

    tasks = [
        asyncio.create_task(binance_stream.start()),
        asyncio.create_task(binance_kline_1m.start()),
        asyncio.create_task(binance_kline_5m.start()),
        asyncio.create_task(polymarket_ws_stream.start()),
        asyncio.create_task(polymarket_clob_ws.start()),
        asyncio.create_task(chainlink_ws_stream.start()),
        asyncio.create_task(update_loop()),
        asyncio.create_task(telegram_poller()),
        asyncio.create_task(entry_watcher()),
        asyncio.create_task(redeem_watcher())
    ]

    yield

    for task in tasks:
        task.cancel()

    binance_stream.close()
    binance_kline_1m.close()
    binance_kline_5m.close()
    polymarket_ws_stream.close()
    polymarket_clob_ws.close()
    chainlink_ws_stream.close()

app = FastAPI(title="Polymarket BTC 15m Assistant [FAK]", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    _ws_clients.add(websocket)
    try:
        init_msg = json.dumps({
            "type": "state_update",
            "data": state["latest_data"],
            "logs": state["logs"][-30:],
            "log_seq": state["log_seq"],
            "redeemable": state["redeemable"],
            "redeem_run": state["redeem_run"],
        })
        await websocket.send_text(init_msg)
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _ws_clients.discard(websocket)

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/settings", response_class=HTMLResponse)
async def get_settings_page(request: Request):
    return templates.TemplateResponse("settings.html", {"request": request})

@app.get("/api/latest")
async def get_latest():
    return state["latest_data"]

@app.get("/api/logs")
async def get_logs():
    return state["logs"]

DOWNLOADABLE = {
    "signals": (SIGNALS_PATH, "text/csv"),
    "trades": (STATE_PATH, "application/json"),
}

@app.get("/api/files")
async def list_files():
    out = []
    for key, (path, _) in DOWNLOADABLE.items():
        exists = os.path.exists(path)
        out.append({
            "key": key,
            "name": os.path.basename(path),
            "exists": exists,
            "size": os.path.getsize(path) if exists else 0,
            "rows": (max(0, sum(1 for _ in open(path, encoding="utf-8", errors="ignore")) - 1)
                     if exists and path.endswith(".csv") else None),
            "modified": (datetime.fromtimestamp(os.path.getmtime(path)).isoformat()
                         if exists else None),
        })
    return out

@app.get("/api/download/{key}")
async def download_file(key: str):
    entry = DOWNLOADABLE.get(key)
    if not entry:
        return JSONResponse({"error": "unknown_file"}, status_code=404)
    path, media = entry
    if not os.path.exists(path):
        return JSONResponse({"error": "not_generated_yet", "path": path}, status_code=404)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base, ext = os.path.splitext(os.path.basename(path))
    return FileResponse(path, media_type=media, filename=f"15m-{base}-{stamp}{ext}")

def _reflect_running_now():
    ts = state["latest_data"].get("trading_state")
    if isinstance(ts, dict):
        ts["running"] = state["running"]

@app.post("/api/start")
async def start_trading():
    state["running"] = True
    _reflect_running_now()
    log_message("Trading STARTED by user")
    await send_telegram("🟢 *Trading STARTED by user*")
    await broadcast_state()
    return {"ok": True, "running": True}

@app.post("/api/stop")
async def stop_trading():
    state["running"] = False
    _reflect_running_now()
    log_message("Trading STOPPED by user")
    await send_telegram("🔴 *Trading STOPPED by user*")
    await broadcast_state()
    return {"ok": True, "running": False}

@app.get("/api/available-series")
async def get_available_series():
    return await data.fetch_available_15m_series()

@app.get("/api/redeemable")
async def get_redeemable():
    return await refresh_redeemable()

@app.post("/api/redeem-all")
async def redeem_all():
    if state["trading_mode"] != "live":
        return {"ok": False, "error": "live_mode_only"}
    if state["redeem_run"]["busy"]:
        return {"ok": False, "error": "already_running"}
    res = await _redeem_all_run()
    return {"ok": True, "result": res}

@app.post("/api/redeem")
async def redeem_single(req: Dict[str, Any]):
    if state["trading_mode"] != "live":
        return {"ok": False, "error": "live_mode_only"}
    condition_id = req.get("condition_id")
    amounts = req.get("amounts", [0.0, 0.0])
    neg_risk = bool(req.get("neg_risk", False))
    if not condition_id:
        return {"ok": False, "error": "missing_condition_id"}
    res = await asyncio.to_thread(clob_trader.redeem, condition_id, amounts, neg_risk)
    if res.get("ok"):
        state["redeem_submitted"][condition_id] = time.time()
        save_state()
        await refresh_redeemable()
        await broadcast_state()
    return res

@app.get("/api/settings")
async def get_settings():
    def mask(v: str) -> str:
        return v[:6] + "..." + v[-4:] if v and len(v) > 10 else v

    masked_pk = mask(settings.PRIVATE_KEY)

    return {
        "mode": settings.MODE,
        "paper_balance_usd": settings.PAPER_BALANCE_USD,
        "private_key": masked_pk,
        "live": {
            "relayer_api_key": mask(settings.RELAYER_API_KEY),
            "alchemy_api_key": mask(settings.ALCHEMY_API_KEY),
            "max_slippage": settings.CLOB_MAX_SLIPPAGE,
            "exit_max_retries": settings.EXIT_MAX_RETRIES,
            "allow_partial_fill": settings.CLOB_ALLOW_PARTIAL_FILL
        },
        "polymarket": {
            "series_id": settings.POLYMARKET_SERIES_ID,
            "gamma_base_url": settings.GAMMA_BASE_URL,
            "clob_base_url": settings.CLOB_BASE_URL,
            "live_ws_url": settings.POLYMARKET_LIVE_DATA_WS_URL,
            "up_label": settings.POLYMARKET_UP_LABEL,
            "down_label": settings.POLYMARKET_DOWN_LABEL
        },
        "trading": {
            "symbol": settings.SYMBOL,
            "risk_type": settings.RISK_TYPE,
            "risk_value": settings.RISK_VALUE
        },
        "ev": {
            "ev_threshold": settings.EV_THRESHOLD,
            "min_prob": settings.MIN_PROB_EV,
            "min_book_liquidity_usd": settings.MIN_BOOK_LIQUIDITY_USD,
            "max_book_age_s": settings.MAX_BOOK_AGE_S
        },
        "flip": {
            "enabled": settings.FLIP_ENABLED,
            "min_conviction": settings.FLIP_MIN_CONVICTION,
            "min_minutes_left": settings.FLIP_MIN_MINUTES_LEFT
        },
        "capital_extractor": {
            "enabled": settings.AUTO_WITHDRAW_ENABLED,
            "trigger_balance": settings.WITHDRAW_TRIGGER_BALANCE,
            "withdraw_amount": settings.WITHDRAW_AMOUNT,
            "recipient_address": settings.WITHDRAW_ADDRESS,
            "auto_resume": settings.WITHDRAW_AUTO_RESUME,
            "resume_after": settings.WITHDRAW_RESUME_AFTER
        },
        "telegram": {
            "enabled": settings.TELEGRAM_ENABLED,
            "bot_token": mask(settings.TELEGRAM_BOT_TOKEN)
        }
    }

@app.post("/api/settings")
async def post_settings(new_settings: Dict[str, Any]):
    global binance_stream, polymarket_ws_stream, chainlink_ws_stream, binance_kline_1m, binance_kline_5m
    old_symbol = settings.SYMBOL

    new_pk = new_settings.get("private_key")
    if new_pk and "..." in new_pk:
        new_settings["private_key"] = settings.PRIVATE_KEY
    elif new_pk:
        from bot.config import normalize_private_key
        try:
            settings.PRIVATE_KEY = normalize_private_key(new_pk)
            new_settings["private_key"] = settings.PRIVATE_KEY
        except Exception as e:
            return {"status": "error", "error": f"invalid_private_key: {e}"}

    cfg_file = CONFIG_PATH if os.path.exists(CONFIG_PATH) else "config.json"
    existing_cfg = {}
    if os.path.exists(cfg_file):
        try:
            with open(cfg_file, "r") as f:
                existing_cfg = json.load(f)
        except Exception:
            existing_cfg = {}

    def deep_merge(base, override):
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                deep_merge(base[k], v)
            else:
                base[k] = v
        return base

    merged_cfg = deep_merge(existing_cfg, new_settings)
    with open(CONFIG_PATH, "w") as f:
        json.dump(merged_cfg, f, indent=2)

    settings.MODE = new_settings.get("mode", settings.MODE)
    settings.PAPER_BALANCE_USD = float(new_settings.get("paper_balance_usd", settings.PAPER_BALANCE_USD))

    if "trading" in new_settings:
        t = new_settings["trading"]
        settings.SYMBOL = t.get("symbol", settings.SYMBOL)
        settings.RISK_TYPE = t.get("risk_type", settings.RISK_TYPE)
        settings.RISK_VALUE = float(t.get("risk_value", settings.RISK_VALUE))

    if "ev" in new_settings:
        e = new_settings["ev"]
        settings.EV_THRESHOLD = float(e.get("ev_threshold", settings.EV_THRESHOLD))
        settings.MIN_PROB_EV = float(e.get("min_prob", settings.MIN_PROB_EV))
        settings.MIN_BOOK_LIQUIDITY_USD = float(e.get("min_book_liquidity_usd", settings.MIN_BOOK_LIQUIDITY_USD))
        if "max_book_age_s" in e:
            settings.MAX_BOOK_AGE_S = float(e["max_book_age_s"])

    if "flip" in new_settings:
        f = new_settings["flip"]
        if "enabled" in f:
            settings.FLIP_ENABLED = bool(f["enabled"])
        settings.FLIP_MIN_CONVICTION = float(f.get("min_conviction", settings.FLIP_MIN_CONVICTION))
        settings.FLIP_MIN_MINUTES_LEFT = float(f.get("min_minutes_left", settings.FLIP_MIN_MINUTES_LEFT))

    if "polymarket" in new_settings:
        p = new_settings["polymarket"]
        settings.POLYMARKET_SERIES_ID = p.get("series_id", settings.POLYMARKET_SERIES_ID)
        settings.POLYMARKET_UP_LABEL = p.get("up_label", settings.POLYMARKET_UP_LABEL)
        settings.POLYMARKET_DOWN_LABEL = p.get("down_label", settings.POLYMARKET_DOWN_LABEL)

    if "live" in new_settings:
        lv = new_settings["live"]
        if "max_slippage" in lv:
            settings.CLOB_MAX_SLIPPAGE = float(lv["max_slippage"])
        if "exit_max_retries" in lv:
            settings.EXIT_MAX_RETRIES = int(lv["exit_max_retries"])
        if "allow_partial_fill" in lv:
            settings.CLOB_ALLOW_PARTIAL_FILL = bool(lv["allow_partial_fill"])

        rk = lv.get("relayer_api_key")
        if rk and "..." not in rk:
            settings.RELAYER_API_KEY = rk
            new_settings.setdefault("relayer", {})["api_key"] = rk
        elif rk:
            lv["relayer_api_key"] = settings.RELAYER_API_KEY
        ak = lv.get("alchemy_api_key")
        if ak and "..." not in ak:
            settings.ALCHEMY_API_KEY = ak
            new_settings.setdefault("chainlink", {})["alchemy_api_key"] = ak
        elif ak:
            lv["alchemy_api_key"] = settings.ALCHEMY_API_KEY

    if "capital_extractor" in new_settings:
        ce = new_settings["capital_extractor"]
        if "enabled" in ce: settings.AUTO_WITHDRAW_ENABLED = bool(ce["enabled"])
        if "trigger_balance" in ce: settings.WITHDRAW_TRIGGER_BALANCE = float(ce["trigger_balance"])
        if "withdraw_amount" in ce: settings.WITHDRAW_AMOUNT = float(ce["withdraw_amount"])
        if "recipient_address" in ce: settings.WITHDRAW_ADDRESS = str(ce["recipient_address"]).strip()
        if "auto_resume" in ce: settings.WITHDRAW_AUTO_RESUME = bool(ce["auto_resume"])
        if "resume_after" in ce: settings.WITHDRAW_RESUME_AFTER = str(ce["resume_after"]).strip()

    if "telegram" in new_settings:
        tg = new_settings["telegram"]
        if "enabled" in tg: settings.TELEGRAM_ENABLED = bool(tg["enabled"])
        tok = tg.get("bot_token")
        if tok and "..." not in tok:
            settings.TELEGRAM_BOT_TOKEN = str(tok).strip()

    clob_trader.reset()

    state["trading_mode"] = settings.MODE
    state["paper_balance"] = settings.PAPER_BALANCE_USD

    if settings.SYMBOL != old_symbol:
        binance_stream.close()
        binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL, on_update=_wake_entry)
        asyncio.create_task(binance_stream.start())

        binance_kline_1m.close()
        binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
        asyncio.create_task(binance_kline_1m.start())

        binance_kline_5m.close()
        binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)
        asyncio.create_task(binance_kline_5m.start())

        await seed_kline_buffers()

        polymarket_ws_stream.close()
        polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
            ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
            symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
        )
        asyncio.create_task(polymarket_ws_stream.start())

        chainlink_ws_stream.close()
        chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))
        asyncio.create_task(chainlink_ws_stream.start())

    await broadcast_state()
    return {"status": "ok"}

@app.post("/api/setup-wallet")
async def setup_wallet():
    try:
        result = await asyncio.to_thread(clob_trader.ensure_setup)
        if result.get("ok"):
            if result.get("skipped"):
                log_message("Wallet setup: already done this session")
            else:
                log_message(f"Wallet setup complete ({result.get('approvals', 0)} approvals)")
        else:
            log_message(f"Wallet setup failed: {result.get('error')}")
        return result
    except Exception as e:
        log_message(f"Wallet setup error: {e}")
        return {"ok": False, "error": str(e)}

@app.post("/api/test-connection")
async def test_connection():
    try:
        result = await asyncio.to_thread(clob_trader.test_connection)
        if result.get("ok"):
            log_message(f"Connection OK — EOA {result.get('eoa')}, trading from "
                        f"{result.get('funder')} (sig type {result.get('chosen_signature_type')})")
        else:
            log_message(f"Connection test failed: {result.get('error')}")
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/api/enable-auto-redeem")
async def enable_auto_redeem():
    try:
        result = await asyncio.to_thread(clob_trader.enable_auto_redeem)
        log_message("Auto-redeem enabled" if result.get("ok")
                    else f"Auto-redeem failed: {result.get('error')}")
        return result
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/health")
async def health():
    return {"status": "ok", "last_update": state["last_update_ts"], "mode": state["trading_mode"],
            "running": state["running"]}

@app.get("/history")
async def get_history():
    return state["trade_history"]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
