""""""
from datetime import datetime
from threading import Thread

from vnpy.trader.setting import SETTINGS
from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.constant import Interval
from vnpy.trader.object import HistoryRequest, ContractData
from vnpy.trader.rqdata import rqdata_client
from vnpy.trader.jqdata import jqdata_client


APP_NAME = "ChartWizard"

EVENT_CHART_HISTORY = "eChartHistory"


class ChartWizardEngine(BaseEngine):
    """"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        rqdata_client.init()

    def query_history(
        self,
        vt_symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        thread = Thread(
            target=self._query_history,
            args=[vt_symbol, interval, start, end]
        )
        thread.start()

    def _query_history(
        self,
        vt_symbol: str,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        contract: ContractData = self.main_engine.get_contract(vt_symbol)

        req = HistoryRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            interval=interval,
            start=start,
            end=end
        )
        data_egine = SETTINGS["data_egine"]

        if contract.history_data:
            data = self.main_engine.query_history(req, contract.gateway_name)
        else:
            if data_egine == "rq":
                if not rqdata_client.inited:
                    rqdata_client.init()
                data = rqdata_client.query_history(req)
            else:
                if not jqdata_client.inited:
                    jqdata_client.init()
                data = jqdata_client.query_history(req)

        event = Event(EVENT_CHART_HISTORY, data)
        self.event_engine.put(event)
