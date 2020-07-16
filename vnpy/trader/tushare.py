import tushare as ts
from vnpy.trader.object import HistoryRequest, BarData
from vnpy.trader.constant import Exchange
from datetime import datetime
from typing import List

class TushareData:
    def __init__(self):
        ts.set_token('8cbd21001f421700fbf3e5a026a07fb2a105633b52cf0c9acb0cec44')
        return
    
    def exchange_bond(self, exchange:Exchange):
        if exchange.value == "CFFEX":
            return "CFX"
        elif exchange.value == "SHFE":
            return "SHF"
        elif exchange.value == "CZCE":
            return "ZCE"
        elif exchange.value == "SSE":
            return "SH"
        elif exchange.value == "SZSE":
            return "SZ"
        else :
            return exchange.value
    
    def tuquery(self, req:HistoryRequest):
        symbol = req.symbol
        exchange = req.exchange
        interval = req.interval
        start = req.start.strftime('%Y%m%d')
        end = req.end.strftime('%Y%m%d')
        tcode = f'{symbol}' + '.' + self.exchange_bond(exchange)
        pro = ts.pro_api()

        if exchange.value == "SSE" or exchange.value == "SZSE":
            if interval.value == "d":
                df = pro.daily(ts_code = tcode, start_date = start, end_date = end)
            elif interval.value == "w":
                df = pro.weekly(ts_code = tcode, start_date = start, end_date = end)
            else:
                return None
        elif exchange.value == "CFFEX" or exchange.value == "SHFE" or exchange.value == "CZCE" or exchange.value == "DCE" or exchange.value == "INE":
            df = pro.fut_daily(ts_code = tcode, start_date = start, end_date = end)
        else:
            return None

        data: List[BarData] = []

        if df is not None:
            for ix, row in df.iterrows():
                date = datetime.strptime(row.trade_date, '%Y%m%d')
                bar = BarData(
                    symbol = symbol,
                    exchange = exchange,
                    interval = interval,
                    datetime = date,
                    open_price = row["open"],
                    high_price = row["high"],
                    low_price=row["low"],
                    close_price=row["close"],
                    volume=row["amount"],
                    gateway_name="TU"
                )
                print(bar)
                data.append(bar)
            return data
    
tusharedata = TushareData()
    