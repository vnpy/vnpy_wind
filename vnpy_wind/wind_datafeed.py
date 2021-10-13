from datetime import timedelta, datetime
from typing import List, Optional
from pytz import timezone

from pandas import DataFrame
from WindPy import w

from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.utility import extract_vt_symbol
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
                open_interest=tp.position,
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
                open_interest=tp.OI,
                gateway_name="WIND"
            )
            bars.append(bar)

        return bars

    def query_tick_history(self, req: HistoryRequest) -> Optional[List[TickData]]:
        """查询Tick数据"""
        if not w.isconnected():
            self.init()

        wind_exchange = EXCHANGE_MAP[req.exchange]
        wind_symbol = f"{req.symbol}.{wind_exchange}"

        fields = [
            "open",
            "high",
            "low",
            "last",
            "volume",
            "turnover",
            "oi",
            "bid1",
            "bid2",
            "bid3",
            "bid4",
            "bid5",
            "ask1",
            "ask2",
            "ask3",
            "ask4",
            "ask5",
        ]

        df: DataFrame = w.wst(
            codes=wind_symbol,
            fields=fields,
            beginTime=req.start,
            endTime=req.end,
            options="",
            usedf=True
        )

        ticks: List[TickData] = []
        for tp in df.itertuples():
            tick = TickData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=tp.Index,
                open_price=tp.open,
                high_price=tp.high,
                low_price=tp.low,
                last_price=tp.last,
                volume=tp.volume,
                turnover=tp.turnover,
                open_interest=tp.oi,
                bid_price_1=tp.bid1,
                bid_price_2=tp.bid2,
                bid_price_3=tp.bid3,
                bid_price_4=tp.bid4,
                bid_price_5=tp.bid5,
                ask_price_1=tp.ask1,
                ask_price_2=tp.ask2,
                ask_price_3=tp.ask3,
                ask_price_4=tp.ask4,
                ask_price_5=tp.ask5,
                gateway_name="WIND"
            )
            ticks.append(tick)

        return ticks
