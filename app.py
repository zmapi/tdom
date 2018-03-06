import zmq
import asyncio
import curses
import argparse
import sys
import traceback
import logging
import json
import select
import signal
from datetime import datetime
from decimal import Decimal
from time import sleep, time, gmtime
from zmapi.codes import error
from pprint import pprint, pformat
from uuid import uuid4
from sortedcontainers import SortedDict


################################## CONSTANTS ##################################

# colors
CP_STATUS = 1
CP_BIDS_BASE = 2
CP_ASKS_BASE = 3
CP_PRICE = 4
CP_TIME = 5
CP_SEPARATOR = 6

MODULE_NAME = "cmdom"


################################### GLOBALS ###################################


class GlobalState:
    pass
g = GlobalState()
g.ctx = zmq.Context()
g.last_status_msg = ""
g.bids = SortedDict(lambda x: -x)
g.asks = SortedDict()
g.trades = []
g.y_pos = 0
g.price_max_len = 0
g.size_max_len = 0
g.best_bid = None
g.best_ask = None

L = logging.root


###############################################################################


def parse_args():
    desc = "curses based dom trader"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("ctl_addr_up",
                        help="address of the upstream ctl socket")
    parser.add_argument("pub_addr_up",
                        help="address of the upstream pub socket")
    parser.add_argument("ticker_id", help="ticker id to subscribe to")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    parser.add_argument("--debug", action="store_true",
                        help="enable debug mode")
    parser.add_argument("--utc", action="store_true", help="show times in UTC")
    parser.add_argument("--num-levels", type=int, default=sys.maxsize,
                        help="number of order book levels")
    parser.add_argument("--log", help="debug log-file path")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args


def setup_logger():
    if not g.args.log:
        return
    f = open(g.args.log, "w")
    logger = logging.root
    logger.setLevel(g.args.log_level)
    logger.handlers.clear()
    fmt = "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s"
    datefmt = "%H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    # convert datetime to utc
    formatter.converter = gmtime
    handler = logging.FileHandler(g.args.log, "w")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def init_zmq():
    g.sock_req = g.ctx.socket(zmq.REQ)
    g.sock_req.setsockopt_string(zmq.IDENTITY, MODULE_NAME)
    g.sock_req.connect(g.args.ctl_addr_up)
    g.sock_sub = g.ctx.socket(zmq.SUB)
    g.sock_sub.connect(g.args.pub_addr_up)


# mutates msg
def pack_msg(msg : dict):
    if "msg_id" not in msg:
        msg["msg_id"] = str(uuid4())
    return (" " + json.dumps(msg)).encode()


def send_recv_command_raw(cmd, content):
    sock = g.sock_req
    msg = dict(command=cmd, content=content)
    msg_bytes = pack_msg(msg)
    sock.send(msg_bytes)
    msg_parts = sock.recv_multipart()
    msg = json.loads(msg_parts[-1].decode())
    error.check_message(msg)
    return msg["content"]


def get_ticker_info():
    content = {"ticker": {"ticker_id": g.args.ticker_id}}
    res = send_recv_command_raw("get_ticker_info", content)
    if len(res) != 1:
        exit("invalid ticker id")
    g.ticker_info = res[0]


def get_converters(data):
    """Get converters that convert between raw price and integer price."""
    if data["float_price"]:
        ts = data["price_tick_size"]
        if isinstance(ts, list):
            # use the smallest tick size
            ts = tick_size[0][1]
        # is this a good way to get number of decimals?
        num_decimals = -Decimal(str(ts)).as_tuple().exponent
        ts = 1.0 * 10 ** -num_decimals
        L.debug("{}: min_tick={}".format(data["ticker_id"], ts))
        def r2i_converter(x):
            return round(x / ts)
        def i2r_converter(x):
            return round(x * ts, num_decimals)
    else:
        # If prices are in int format no conversion needed =>
        # return identity functions.
        def r2i_converter(x):
            return x
        def i2r_converter(x):
            return x
    g.r2i_conv = r2i_converter
    g.i2r_conv = i2r_converter


def get_snapshot():
    L.debug("getting snapshot ...")
    content = {
        "ticker_id": g.args.ticker_id,
        "order_book_levels": 1000000000,
        "quotes": True,
        "historical_trades": 1000000000,
    }
    res = send_recv_command_raw("get_snapshot", content)
    # quotes = res.get("quotes", {})
    # if "bid_price" in res["quotes"]:
    #     g.best_bid = res["quotes"]["bid_price"]
    # if "ask_price" in res["quotes"]:
    #     g.best_ask = res["quotes"]["ask_price"]
    ob = res.get("order_book", {})
    bids = ob.get("bids", [])
    for lvl in bids:
        i_price = g.r2i_conv(lvl["price"])
        g.bids[i_price] = lvl["size"]
    asks = ob.get("asks", [])
    for lvl in asks:
        i_price = g.r2i_conv(lvl["price"])
        g.asks[i_price] = lvl["size"]
    if g.bids and g.best_bid is None:
        g.best_bid = g.i2r_conv(g.bids.iloc[0])
    if g.asks and g.best_ask is None:
        g.best_ask = g.i2r_conv(g.asks.iloc[0])
    trades = res.get("historical_trades", [])
    for trade in trades:
        # TODO: handle zmapi native aggressor (to be formally defined ...)
        if "aggressor" not in trade:
            trade["aggressor"] = 0
        g.trades.append(trade)
    L.debug("{} bids, {} asks, {} trades"
            .format(len(g.bids), len(g.asks), len(g.trades)))


def subscribe():
    ticker_id = g.args.ticker_id
    g.sock_sub.subscribe(ticker_id.encode())
    content = {
        "ticker_id": ticker_id,
        "trades_speed": 5,
        "order_book_speed": 5,
        "order_book_levels": g.args.num_levels,
        "emit_quotes": True,
    }
    res = send_recv_command_raw("modify_subscription", content)
    L.debug("subscribed:\n{}".format(pformat(res)))


def echo_status(s):
    g.last_status_msg = s
    y_max, x_max = g.screen.getmaxyx()
    y = y_max - 1
    s = s.ljust(x_max - 1)
    g.screen.addstr(y, 0, s, curses.color_pair(CP_STATUS))
    g.screen.refresh()


def init_colors():
    curses.init_pair(CP_STATUS, 0, 251)
    curses.init_pair(CP_BIDS_BASE, 20, -1)
    curses.init_pair(CP_ASKS_BASE, 124, -1)
    curses.init_pair(CP_PRICE, 0, -1)
    curses.init_pair(CP_TIME, 240, -1)
    curses.init_pair(CP_SEPARATOR, -1, 254)


def restore_terminal():
    g.screen.keypad(False)
    curses.echo()
    curses.nocbreak()
    curses.endwin()


def exit(s, status_code=1):
    restore_terminal()
    print(s)
    sys.exit(status_code)


def handle_book(data, book_name):
    data = data.get(book_name)
    if not data:
        return
    book = g.__dict__[book_name]
    for lvl in data:
        g.price_max_len = max(g.price_max_len, len(str(lvl["price"])))
        g.size_max_len = max(g.size_max_len, len(str(lvl["size"])))
        i_price = g.r2i_conv(lvl["price"])
        if lvl["size"] == 0:
            book.pop(i_price, None)
            continue
        book[i_price] = lvl["size"]
    i_prices_to_del = book.iloc[g.args.num_levels:]
    for i_price in i_prices_to_del:
        del book[i_price]


def handle_depth_update(msg):
    handle_book(msg, "bids")
    handle_book(msg, "asks")


def handle_trade(msg):
    if "timestamp" not in msg:
        msg["timestamp"] = time()
    # TODO: handle zmapi native aggressor (to be formally defined ...)
    msg["aggressor"] = 0
    if g.best_ask is not None and msg["price"] >= g.best_ask:
        msg["aggressor"] = 1
    elif g.best_bid is not None and msg["price"] <= g.best_bid:
        msg["aggressor"] = -1
    g.trades.append(msg)
    L.debug("{} trades".format(len(g.trades)))

def handle_quote(msg):
    g.best_bid = msg.get("bid_price", g.best_bid)
    g.best_ask = msg.get("ask_price", g.best_ask)
    L.debug("{}/{}".format(g.best_bid, g.best_ask))

def refresh_screen():

    g.screen.clear()
    y_max, x_max = g.screen.getmaxyx()
    y_max -= 1  # space for status bar

    # vertical padding between columns
    padding = 2
    x_dom_price = 0
    x_dom_size = g.price_max_len + padding

    # render asks
    start_y = y_max // 2 + 1 - g.y_pos
    for i in range(len(g.asks)):
        y = start_y - i
        if y >= y_max:
            continue
        if y < 0:
            break
        i_price = g.asks.iloc[i]
        size = g.asks[i_price]
        s_price = str(g.i2r_conv(i_price)).ljust(g.price_max_len)
        s_size = str(size).rjust(g.size_max_len)
        g.screen.addstr(y, x_dom_price, s_price, curses.color_pair(CP_PRICE))
        g.screen.addstr(y, x_dom_size, s_size,
                        curses.color_pair(CP_ASKS_BASE))

    # render bids
    start_y = y_max // 2 - g.y_pos
    for i in range(len(g.bids)):
        y = start_y + i
        if y < 0:
            continue
        if y >= y_max:
            break
        i_price = g.bids.iloc[i]
        size = g.bids[i_price]
        s_price = str(g.i2r_conv(i_price)).ljust(g.price_max_len)
        s_size = str(size).rjust(g.size_max_len)
        g.screen.addstr(y, x_dom_price, s_price, curses.color_pair(CP_PRICE))
        g.screen.addstr(y, x_dom_size, s_size,
                        curses.color_pair(CP_BIDS_BASE))

    # render separator
    x_separator = x_dom_size + g.size_max_len + padding
    for y in range(y_max):
        g.screen.addstr(y, x_separator, " ", curses.color_pair(CP_SEPARATOR))
    
    # figure out the right date format based on trades data
    dtfmt = "%H:%M:%S"
    dt_len = 8
    if g.trades:
        start_i = max(0, len(g.trades) - y_max - 2)
        sel_trades = g.trades[start_i:]
        start_trade = None
        for i in range(start_i, len(g.trades)):
            t = g.trades[i]
            if "timestamp" in t:
                start_trade = t
                break
        if start_trade:
            ts_diff = sel_trades[-1]["timestamp"] - start_trade["timestamp"]
            if ts_diff >= 60 * 60 * 24:  # 24h
                dt_len = 14
                dtfmt = "%m-%d %H:%M:%S"

    # render trades column
    for y in range(y_max):
        i = len(g.trades) - 1 - y
        if i < 0:
            break
        trade = g.trades[i]
        s_price = str(trade["price"]).ljust(g.price_max_len)
        s_size = str(trade["size"]).rjust(g.size_max_len)
        x_time = x_separator + 1 + padding
        x_price =  x_time + dt_len + padding
        x_size = x_price + padding + g.price_max_len
        ts = trade.get("timestamp")
        if ts:
            if g.args.utc:
                dt = datetime.utcfromtimestamp(ts)
            else:
                dt = datetime.fromtimestamp(ts)
            s_time = dt.strftime(dtfmt)
            # s_time += "." + str(round(dt.microsecond / 1000)).zfill(3)
            g.screen.addstr(y, x_time, s_time, curses.color_pair(CP_TIME))
        if trade["aggressor"] < 0:
            cp = CP_ASKS_BASE
        elif trade["aggressor"] > 0:
            cp = CP_BIDS_BASE
        else:
            cp = CP_PRICE
        g.screen.addstr(y, x_price, s_price, curses.color_pair(cp))
        g.screen.addstr(y, x_size, s_size, curses.color_pair(CP_PRICE))

    # redraw status
    echo_status(g.last_status_msg)


def start_gui():

    # do not print out key presses unless specifically asked to
    curses.noecho()
    # cbreak mode: respond to key presses without pressing enter
    curses.cbreak()
    # activate navigation key shortcuts
    g.screen.keypad(True)
    # no cursor is needed
    curses.curs_set(False)
    # enable support for transparent color (-1)
    curses.use_default_colors()
    # enable colors
    curses.start_color()
    if curses.has_colors():
        if curses.COLORS < 255:
            exit("terminal with 255 color support is required")
    else:
        exit("terminal does not support colors")
    init_colors()

    echo_status("[{}] getting ticker info ...".format(g.args.ticker_id))
    get_ticker_info()
    get_converters(g.ticker_info) 
    echo_status("[{}] getting snapshot ...".format(g.args.ticker_id))
    get_snapshot()
    echo_status("[{}] subscribing ...".format(g.args.ticker_id))
    res = subscribe()
    s = "[{}] subscribed".format(g.args.ticker_id)
    if g.args.num_levels < sys.maxsize:
        s += " ({})".format(g.args.num_levels)
    echo_status(s)

    # Threading based design with message passing and ui manipulating event
    # loop running on main thread is not easy to implement here. When terminal
    # gets resized, SIGWINCH is emitted and it's automatically caught by curses
    # library. Curses library emits "KEY_RESIZE" special key stroke to signal
    # the application about this. This signal is only emitted to the main
    # thread and if stdin listening loop (stdscr.getkey) is running on another
    # thread it won't be emitted and curses internal structures won't be
    # updated. This desing decision by curses library makes it hard to
    # implement any threading model without modifying source code of curses
    # library itself on C level. Falling back on polling desing, which seems to
    # be fine for a lightweight application.

    # TODO: adjust polling frequency
    g.screen.nodelay(True)

    while True:

        refreshing = False

        # handle curses events
        key = None
        try:
            key = g.screen.getkey()
        except Exception:
            pass
        if key is not None:
            L.debug("{!r}".format(key))
        if key == "KEY_RESIZE":
            L.debug("handling resize...")
            refreshing = True
        elif key == "KEY_UP":
            g.y_pos -= 1
            refreshing = True
        elif key == "KEY_DOWN":
            g.y_pos += 1
            refreshing = True
        elif key == " ":
            g.y_pos = 0
            refreshing = True
        elif key == "KEY_PPAGE":
            y_max, _ = g.screen.getmaxyx()
            g.y_pos -= y_max // 2
            refreshing = True
        elif key == "KEY_NPAGE":
            y_max, _ = g.screen.getmaxyx()
            g.y_pos += y_max // 2
            refreshing = True

        # handle zmq messages
        try:
            msg_parts = g.sock_sub.recv_multipart(zmq.NOBLOCK)
        except zmq.error.Again:
            pass
        else:
            topic, msg = msg_parts
            try:
                msg = json.loads(msg.decode())
                if topic[-1] == 1:
                    handle_depth_update(msg)
                    refreshing = True
                elif topic[-1] == 2:
                    handle_trade(msg)
                    refreshing = True
                elif topic[-1] == 3:
                    handle_quote(msg)
            except Exception:
                L.exception("exception on sub_listener:\n", exc_info=True)

        if refreshing:
            refresh_screen()


def main_2(stdscr):
    setup_logger()
    L.info("initializing zmq ...")
    init_zmq()
    L.info("zmq initialized")
    start_gui()


def main(stdscr):
    g.screen = stdscr
    # is this necessary? stderr gets ignored outherwise?
    sys.stderr = sys.stdout
    try:
        main_2(stdscr)
    except KeyboardInterrupt:
        restore_terminal()
        sys.exit(0)
    except Exception as err:
        restore_terminal()
        traceback.print_exc()
        sys.exit(1)


# argparse need to be ran before curses.wrapper, otherwise errors won't
# get printed to output.
g.args = parse_args()
curses.wrapper(main)


