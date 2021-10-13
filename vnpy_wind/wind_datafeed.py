from datetime import timedelta, datetime
from typing import List, Optional
from pytz import timezone
from math import isnan

from WindPy import w

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.datafeed import BaseDatafeed


CHINA_TZ = timezone("Asia/Shanghai")

EXCHANGE_MAP = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
    Exchange.CFFEX: "CFE",
    Exchange.SHFE: "SHF",
    Exchange.CZCE: "CZC",
    Exchange.DCE: "DCE",
}

INTERVAL_MAP = {
    Interval.MINUTE: "1",
    Interval.HOUR: "60"
}

SHIFT_MAP = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
}


class WindDatafeed(BaseDatafeed):
    """万得数据服务接口"""

    def init(self) -> bool:
        """初始化"""
        if w.isconnected():
            return True

        data = w.start()
        if data.ErrorCode:
            return False
        return True

    def query_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询K线数据"""
        if not w.isconnected():
            self.init()

        if req.interval == Interval.DAILY:
            return self.query_daily_bar_history(req)
        else:
            return self.query_intraday_bar_history(req)

    def query_intraday_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询日内K线数据"""
        wind_exchange = EXCHANGE_MAP[req.exchange]
        wind_symbol = f"{req.symbol}.{wind_exchange}"

        fields = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amt",
            "oi"
        ]

        wind_interval = INTERVAL_MAP[req.interval]
        options = f"BarSize={wind_interval}"

        error, df = w.wsi(
            codes=wind_symbol,
            fields=fields,
            beginTime=req.start,
            endTime=req.end,
            options=options,
            usedf=True
        )

        if error:
            return []

        bars: List[BarData] = []
        for tp in df.itertuples():
            dt = tp.Index.to_pydatetime()

            if isnan(tp.position):
                open_interest = 0
            else:
                open_interest = tp.position

            bar = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                interval=req.interval,
                datetime=CHINA_TZ.localize(dt),
                open_price=tp.open,
                high_price=tp.high,
                low_price=tp.low,
                close_price=tp.close,
                volume=tp.volume,
                turnover=tp.amount,
                open_interest=open_interest,
                gateway_name="WIND"
            )
            bars.append(bar)

        return bars

    def query_daily_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询日K线数据"""
        wind_exchange = EXCHANGE_MAP[req.exchange]
        wind_symbol = f"{req.symbol}.{wind_exchange}"

        fields = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amt",
            "oi"
        ]

        error, df = w.wsd(
            codes=wind_symbol,
            fields=fields,
            beginTime=req.start,
            endTime=req.end,
            options="",
            usedf=True
        )

        if error:
            return []

        bars: List[BarData] = []
        for tp in df.itertuples():
            dt = datetime.combine(tp.Index, datetime.min.time())

            if isnan(tp.OI):
                open_interest = 0
            else:
                open_interest = tp.OI

            bar = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                interval=req.interval,
                datetime=CHINA_TZ.localize(dt),
                open_price=tp.OPEN,
                high_price=tp.HIGH,
                low_price=tp.LOW,
                close_price=tp.CLOSE,
                volume=tp.VOLUME,
                turnover=tp.AMT,
                open_interest=open_interest,
                gateway_name="WIND"
            )
            bars.append(bar)

        return bars
