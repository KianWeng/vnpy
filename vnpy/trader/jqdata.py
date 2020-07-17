from datetime import timedelta
from typing import List, Optional
from pytz import timezone
from numpy import array

from .setting import SETTINGS
from .constant import Exchange, Interval
from .object import BarData, HistoryRequest

from jqdatasdk import *

INTERVAL_VT2JQ = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "60m",
    Interval.DAILY: "1d",
}

INTERVAL_ADJUSTMENT_MAP = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta()         # no need to adjust for daily bar
}

CHINA_TZ = timezone("Asia/Shanghai")

class JqdataClient():
    """
    Client for querying history data from JQData.
    """

    def __init__(self):
        self.username: str = SETTINGS["jqdata.username"]
        self.password: str = SETTINGS["jqdata.password"]

        self.inited: bool = False
        self.symbols: array = None

    def init(self, username: str = "", password: str = "") -> bool:
        """"""
        if self.inited:
            return True

        if username and password:
            self.username = username
            self.password = password

        if not self.username or not self.password:
            return False

        try:
            auth(self.username, self.password)

            type = ["stock", "fund", "index", "futures","options", "etf"]
            df = get_all_securities(types = type)
            self.symbols = array(df.index)
        except:
            return False

        self.inited = True
        return True

    def to_jq_symbol(self, symbol: str, exchange: Exchange) -> str:
        """
        CZCE product of JQData has symbol like "TA1905" while
        vt symbol is "TA905.CZCE" so need to add "1" in symbol.
        """
        # Equity
        if exchange == Exchange.SSE:
            jq_symbol = f"{symbol}.XSHG"
        elif exchange == Exchange.SZSE:
            jq_symbol = f"{symbol}.XSHE"
        elif exchange == Exchange.CFFEX:
            jq_symbol = f"{symbol}.CCFX"
        elif exchange == Exchange.DCE:
            jq_symbol = f"{symbol}.XDCE"
        elif exchange == Exchange.SGE:
            jq_symbol = f"{symbol}.XSGE"    
        elif exchange == Exchange.CZCE:
            jq_symbol = f"{symbol}.XZCE"
        else:
            jq_symbol = f"{symbol}.XINE"

        return jq_symbol

    def query_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """
        Query history bar data from JQData.
        """
        if self.symbols is None:
            return None

        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start
        end = req.end

        jq_symbol = self.to_jq_symbol(symbol, exchange)
        if jq_symbol not in self.symbols:
            return None

        jq_interval = INTERVAL_VT2JQ.get(interval)
        if not jq_interval:
            return None

        # For adjust timestamp from bar close point (RQData) to open point (VN Trader)
        adjustment = INTERVAL_ADJUSTMENT_MAP[interval]

        # For querying night trading period data
        end += timedelta(1)

        # Only query open interest for futures contract
        fields = ["open", "high", "low", "close", "volume"]
        if not symbol.isdigit():
            fields.append("open_interest")

        df = get_price(
            jq_symbol,
            frequency=jq_interval,
            fields=fields,
            start_date=start,
            end_date=end,
        )

        data: List[BarData] = []

        if df is not None:
            for ix, row in df.iterrows():
                dt = row.name.to_pydatetime() - adjustment
                dt = dt.replace(tzinfo=CHINA_TZ)

                bar = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=row["open"],
                    high_price=row["high"],
                    low_price=row["low"],
                    close_price=row["close"],
                    volume=row["volume"],
                    open_interest=row.get("open_interest", 0),
                    gateway_name="JQ"
                )

                data.append(bar)

        return data

jqdata_client = JqdataClient()