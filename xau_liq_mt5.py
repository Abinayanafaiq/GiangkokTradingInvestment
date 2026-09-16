# -*- coding: utf-8 -*-
"""
XAUUSDT Binance Liquidation -> MetaTrader 5 (PC) - MARTINGALE
==============================================================
Sumber sinyal : Binance Futures liquidation stream (wss://fstream.binance.com/ws/xauusdt@forceOrder)
Eksekusi      : MetaTrader 5 di PC (library MetaTrader5 untuk Python)

Aturan strategi:
  - Liquidation BUY  di Binance -> kita SELL di MT5
    Liquidation SELL di Binance -> kita BUY  di MT5
    (INVERT_SIGNAL = True untuk membalik arah)
  - MARTINGALE: setiap event liquidation membuka posisi baru selama cycle belum selesai.
    Lot bertambah tiap 3 order: 0.05, 0.05, 0.05, 0.06, 0.06, 0.06, 0.07, 0.07, 0.07, ...
  - Take Profit:
      1 posisi terbuka  -> close saat profit posisi >= TP_SINGLE_USD  ($25)
      >= 2 posisi       -> close SEMUA saat total floating >= TP_GLOBAL_USD ($50)
  - Daily loss limit: jika (realized hari ini + floating) <= -DAILY_LOSS_LIMIT ($2000)
    -> close semua posisi, bot berhenti buka order sampai besok (otomatis reset).
  - Cycle baru dimulai otomatis setelah TP global / cut loss (lot kembali 0.05).

Cara pakai:
  1. Install:  pip install MetaTrader5 websocket-client
  2. MT5 PC terbuka, Algo Trading aktif.
  3. Jalankan:  python xau_liq_mt5.py
  4. Untuk uji tanpa order nyata: set DRY_RUN = True.
"""

import json
import logging
import os
import queue
import threading
import time
from datetime import date, datetime, timedelta

import websocket

# ============================== KONFIGURASI ==============================

# --- Binance ---
BINANCE_SYMBOL   = "xauusdt"   # simbol liquidation feed Binance (huruf kecil)
# Catatan: gunakan domain binancefuture.com (alias resmi fstream.binance.com).
# Dari beberapa jaringan, domain fstream.binance.com membatasi feed data tertentu
# (termasuk liquidation), sedangkan alias binancefuture.com lolos.
WS_HOST          = "fstream.binancefuture.com"
# Langganan: hanya liquidation XAUUSDT (event simbol lain tidak dilanggar & tidak di-log).
WS_URL           = f"wss://{WS_HOST}/stream?streams={BINANCE_SYMBOL}@forceOrder"
MIN_NOTIONAL_USD = 0.0         # minimal nilai liquidation (qty x harga) agar jadi sinyal; 0 = semua
SIGNAL_COOLDOWN  = 5.0         # jeda minimal antar event diproses (hindari duplikat partial fill)
INVERT_SIGNAL    = False       # True = ikuti arah liquidation (BUY-liq -> BUY), False = counter

# --- MetaTrader 5 ---
MT5_SYMBOL       = "XAUUSD"    # nama simbol emas di broker (coba "XAUUSDT"/"GOLD"/"XAUUSDm" jika gagal)
MT5_LOGIN        = 416368768   # login akun
MT5_PASSWORD     = "Cahbagus123@"
MT5_SERVER       = "Exness-MT5Trial14"
MT5_PATH         = None        # contoh: r"C:\Program Files\MetaTrader 5\terminal64.exe" (None = otomatis)

# --- Strategi ---
LOT_START        = 0.05        # lot order pertama dalam cycle
LOT_STEP         = 0.01        # kenaikan lot
LOT_STEP_GROUP   = 3           # tiap 3 order lot naik 1 step: 0.05,0.05,0.05,0.06,0.06,0.06,...
TP_SINGLE_USD    = 25.0        # TP saat hanya 1 posisi terbuka (USD)
TP_GLOBAL_USD    = 50.0        # TP global saat >= 2 posisi terbuka (USD, total floating)
DAILY_LOSS_LIMIT = 2000.0      # jika (realized hari ini + floating) <= -nilai ini: cut loss semua + stop sampai besok
MAX_POSITIONS    = 30          # pengaman: batas jumlah posisi terbuka
DEVIATION        = 50          # slippage maksimal (poin)
MAGIC_NUMBER     = 90210

DRY_RUN          = False       # True = hanya log sinyal & simulasi, tidak kirim order ke MT5

# =========================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("xau-liq-mt5")

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state.json")
signal_queue = queue.Queue(maxsize=100)

_state = {"date": date.today().isoformat(), "stopped": False}
_realized_today = 0.0
_last_realized_calc = 0.0
_last_signal_time = 0.0
_last_status_log = 0.0


# ------------------------------ State harian ------------------------------

def load_state():
    global _state
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            saved = json.load(f)
        if saved.get("date") == date.today().isoformat():
            _state.update(saved)
    except (OSError, ValueError):
        pass
    _state["date"] = date.today().isoformat()
    save_state()


def save_state():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(_state, f)
    except OSError as exc:
        log.error("Gagal simpan state: %s", exc)


# ------------------------------ MetaTrader 5 ------------------------------

def mt5_connect():
    import MetaTrader5 as mt5

    kwargs = {}
    if MT5_PATH:
        kwargs["path"] = MT5_PATH
    if MT5_LOGIN and MT5_PASSWORD and MT5_SERVER:
        kwargs.update({"login": MT5_LOGIN, "password": MT5_PASSWORD, "server": MT5_SERVER})

    if not mt5.initialize(**kwargs):
        raise RuntimeError(f"MT5 initialize gagal: {mt5.last_error()}")

    acc = mt5.account_info()
    if acc is None:
        raise RuntimeError(f"Tidak ada akun aktif: {mt5.last_error()}")
    log.info("MT5 terhubung: akun %s (%s), server %s, balance %.2f %s",
             acc.login, acc.name, acc.server, acc.balance, acc.currency)

    if not mt5.symbol_select(MT5_SYMBOL, True):
        raise RuntimeError(f"Simbol {MT5_SYMBOL} tidak tersedia di broker ini. Cek nama simbolnya.")
    log.info("Simbol aktif: %s", MT5_SYMBOL)


def _pick_filling(sym_info, mt5):
    flags = sym_info.filling_mode
    if flags & 1:
        return mt5.ORDER_FILLING_FOK
    if flags & 2:
        return mt5.ORDER_FILLING_IOC
    return mt5.ORDER_FILLING_RETURN


def next_lot(count):
    """Lot untuk order ke-(count+1) dalam cycle: 0.05,0.05,0.05,0.06,0.06,0.06,..."""
    lot = LOT_START + LOT_STEP * (count // LOT_STEP_GROUP)
    return round(lot, 2)


def positions_of_bot(mt5):
    pos = mt5.positions_get(symbol=MT5_SYMBOL)
    if pos is None:
        return []
    return [p for p in pos if p.magic == MAGIC_NUMBER]


def calc_realized_today(mt5):
    """Realized P/L hari ini (profit+swap+commission) dari history deals dengan magic bot ini."""
    midnight = datetime.combine(date.today(), datetime.min.time())
    deals = mt5.history_deals_get(int(midnight.timestamp()), int(time.time()) + 60)
    if deals is None:
        return 0.0
    return sum(d.profit + d.swap + d.commission for d in deals if d.magic == MAGIC_NUMBER)


def open_market(mt5, direction: str, reason: str):
    count = len(positions_of_bot(mt5))
    if count >= MAX_POSITIONS:
        log.warning("Posisi sudah %d (maks %d), order baru dilewati.", count, MAX_POSITIONS)
        return

    sym = mt5.symbol_info(MT5_SYMBOL)
    tick = mt5.symbol_info_tick(MT5_SYMBOL)
    if sym is None or tick is None:
        log.error("Gagal ambil info/tick %s: %s", MT5_SYMBOL, mt5.last_error())
        return

    is_buy = direction == "buy"
    lot = next_lot(count)
    price = tick.ask if is_buy else tick.bid

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": MT5_SYMBOL,
        "volume": float(lot),
        "type": mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL,
        "price": price,
        "deviation": DEVIATION,
        "magic": MAGIC_NUMBER,
        "comment": f"liq#{count + 1}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": _pick_filling(sym, mt5),
    }
    result = mt5.order_send(request)
    if result is None:
        log.error("order_send None, last_error=%s", mt5.last_error())
        return
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        log.error("Order %s %.2f lot GAGAL retcode=%s (%s)",
                  direction.upper(), lot, result.retcode, result.comment)
        return

    log.info("OPEN %s %s %.2f lot @ %s | posisi ke-%d cycle | (%s)",
             direction.upper(), MT5_SYMBOL, lot, result.price, count + 1, reason)


def close_position(mt5, p):
    sym = mt5.symbol_info(MT5_SYMBOL)
    tick = mt5.symbol_info_tick(MT5_SYMBOL)
    if sym is None or tick is None:
        log.error("Gagal ambil tick utk close: %s", mt5.last_error())
        return False

    is_buy = p.type == mt5.POSITION_TYPE_BUY
    price = tick.bid if is_buy else tick.ask
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": MT5_SYMBOL,
        "position": p.ticket,
        "volume": p.volume,
        "type": mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY,
        "price": price,
        "deviation": DEVIATION,
        "magic": MAGIC_NUMBER,
        "comment": "liq-close",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": _pick_filling(sym, mt5),
    }
    result = mt5.order_send(request)
    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        code = result.retcode if result else mt5.last_error()
        log.error("Close #%s GAGAL retcode=%s", p.ticket, code)
        return False
    log.info("CLOSE #%s %.2f lot, profit=%.2f", p.ticket, p.volume, p.profit + p.swap)
    return True


def close_all(mt5, reason: str):
    for attempt in range(3):
        positions = positions_of_bot(mt5)
        if not positions:
            return
        log.info("Close ALL (%d posisi) - %s [percobaan %d]", len(positions), reason, attempt + 1)
        for p in positions:
            close_position(mt5, p)
        time.sleep(1)
    remaining = positions_of_bot(mt5)
    if remaining:
        log.error("Masih ada %d posisi gagal close, coba manual!", len(remaining))


# ------------------------------ Binance WS (thread) ------------------------------

def handle_liquidation(payload: dict):
    global _last_signal_time

    order = payload.get("o", {})
    if not order:
        return

    sym = (order.get("s") or "").upper()
    side = (order.get("S") or "").upper()          # BUY = short kena likuidasi, SELL = long kena likuidasi
    qty = float(order.get("q") or 0)
    avg_price = float(order.get("ap") or order.get("p") or 0)
    status = (order.get("X") or "").upper()
    notional = qty * avg_price

    # Event simbol lain diabaikan total (tidak di-log agar tidak spam)
    if sym != BINANCE_SYMBOL.upper():
        return

    if status != "FILLED":
        return

    event_time = datetime.fromtimestamp(
        (order.get("T") or time.time() * 1000) / 1000).strftime("%Y-%m-%d %H:%M:%S")
    passed = notional >= MIN_NOTIONAL_USD

    log.info("Liquidation %s %s | jam=%s | qty=%.3f | notional=$%.0f | threshold=$%.0f -> %s",
             sym, side, event_time, qty, notional, MIN_NOTIONAL_USD,
             "ORDER AKTIF" if passed else "di bawah threshold, order tidak dibuka")

    if not passed:
        return

    now = time.time()
    if now - _last_signal_time < SIGNAL_COOLDOWN:
        log.info("Sinyal %s $%.0f dilewati (cooldown).", side, notional)
        return
    _last_signal_time = now

    if INVERT_SIGNAL:
        direction = "buy" if side == "BUY" else "sell"
    else:
        direction = "sell" if side == "BUY" else "buy"

    reason = f"liq {side} ${notional:,.0f} qty={qty:.3f}"
    try:
        signal_queue.put_nowait((direction, reason))
    except queue.Full:
        log.warning("Antrian sinyal penuh, sinyal dibuang.")


def on_message(ws, message):
    try:
        data = json.loads(message)
    except (ValueError, TypeError):
        return
    if "stream" in data and isinstance(data.get("data"), dict):
        data = data["data"]                      # unwrap format combined stream
    if data.get("e") == "forceOrder":
        handle_liquidation(data)


def on_error(ws, error):
    log.error("WebSocket error: %s", error)


def on_close(ws, code, msg):
    log.warning("WebSocket tertutup (code=%s, msg=%s). Reconnect 5 detik...", code, msg)


def on_open(ws):
    log.info("Terhubung ke Binance liquidation stream: %s", WS_URL)


def _ssl_options():
    """SSL options: pakai CA bundle certifi (mengenali Amazon CA yang dipakai
    fstream.binancefuture.com). Fallback ke default bila certifi tidak ada."""
    try:
        import certifi
        return {"ca_certs": certifi.where()}
    except ImportError:
        return None


def run_ws():
    while True:
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        try:
            ws.run_forever(ping_interval=20, ping_timeout=10, sslopt=_ssl_options())
        except Exception as exc:
            log.error("run_forever exception: %s", exc)
        time.sleep(5)


# ------------------------------ Monitor loop (thread utama) ------------------------------

def monitor_loop(mt5):
    """Loop utama: kelola TP, daily loss limit, dan buka posisi dari antrian sinyal."""
    global _realized_today, _last_realized_calc, _last_status_log

    while True:
        today = date.today().isoformat()
        if _state["date"] != today:
            log.info("=== HARI BARU (%s): bot lanjut trading lagi ===", today)
            _state["date"] = today
            _state["stopped"] = False
            save_state()

        positions = positions_of_bot(mt5)
        n = len(positions)
        floating = sum(p.profit + p.swap for p in positions)

        if time.time() - _last_realized_calc >= 10:
            _realized_today = calc_realized_today(mt5)
            _last_realized_calc = time.time()

        day_pl = _realized_today + floating

        # --- Daily loss limit: cut loss semua, stop sampai besok ---
        if not _state["stopped"] and day_pl <= -DAILY_LOSS_LIMIT:
            log.warning("DAILY LOSS LIMIT tercapai: P/L hari ini = $%.2f (limit -$%.0f)",
                        day_pl, DAILY_LOSS_LIMIT)
            close_all(mt5, "daily loss limit")
            _realized_today = calc_realized_today(mt5)
            _state["stopped"] = True
            save_state()
            positions = positions_of_bot(mt5)
            n = len(positions)
            floating = sum(p.profit + p.swap for p in positions)
            day_pl = _realized_today + floating

        # --- Take Profit ---
        if n == 1 and floating >= TP_SINGLE_USD:
            log.info("TP SINGLE: profit 1 posisi $%.2f >= $%.2f", floating, TP_SINGLE_USD)
            close_all(mt5, "TP single")
        elif n >= 2 and floating >= TP_GLOBAL_USD:
            log.info("TP GLOBAL: floating %d posisi $%.2f >= $%.2f", n, floating, TP_GLOBAL_USD)
            close_all(mt5, "TP global")

        # --- Buka posisi baru dari antrian sinyal (martingale) ---
        opened = False
        while not _state["stopped"] and not signal_queue.empty():
            direction, reason = signal_queue.get_nowait()
            if DRY_RUN:
                log.info("[DRY RUN] Sinyal %s (%s) -> tidak ada order.", direction.upper(), reason)
                continue
            open_market(mt5, direction, reason)
            opened = True
        if opened:
            n = len(positions_of_bot(mt5))
            floating = sum(p.profit + p.swap for p in positions_of_bot(mt5))
            day_pl = _realized_today + floating

        # --- Status berkala ---
        if time.time() - _last_status_log >= 30:
            _last_status_log = time.time()
            next_lot_preview = next_lot(n)
            status = "STOP (lanjut besok)" if _state["stopped"] else "aktif"
            log.info("Status [%s] posisi=%d floating=$%.2f realized_today=$%.2f day_pl=$%.2f lot_berikutnya=%.2f",
                     status, n, floating, _realized_today, day_pl, next_lot_preview)

        time.sleep(1)


def main():
    load_state()
    if _state["stopped"]:
        log.warning("Hari ini daily loss limit SUDAH tercapai sebelumnya. Bot menunggu besok untuk trading.")

    mt5_connect()
    import MetaTrader5 as mt5

    if DRY_RUN:
        log.info("DRY_RUN=True: sinyal hanya dicatat, tidak ada order nyata.")

    ws_thread = threading.Thread(target=run_ws, daemon=True)
    ws_thread.start()

    log.info("Strategi: martingale lot %.2f(+%.2f tiap %d order) | TP 1 posisi $%.0f | TP global %d+ posisi $%.0f | loss limit harian -$%.0f | invert=%s",
             LOT_START, LOT_STEP, LOT_STEP_GROUP, TP_SINGLE_USD, TP_GLOBAL_USD, TP_GLOBAL_USD,
             DAILY_LOSS_LIMIT, INVERT_SIGNAL)
    try:
        monitor_loop(mt5)
    except KeyboardInterrupt:
        log.info("Dihentikan oleh user. (Posisi terbuka TIDAK ditutup otomatis.)")


if __name__ == "__main__":
    main()
