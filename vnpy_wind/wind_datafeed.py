from datetime import datetime
from typing import List, Dict, Optional, Callable
from math import isnan

from WindPy import w

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.utility import ZoneInfo


CHINA_TZ = ZoneInfo("Asia/Shanghai")


EXCHANGE_MAP: Dict[Exchange, str] = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
    Exchange.CFFEX: "CFE",
    Exchange.SHFE: "SHF",
    Exchange.CZCE: "CZC",
    Exchange.DCE: "DCE",
}


INTERVAL_MAP: Dict[Interval, str] = {
    Interval.MINUTE: "1",
    Interval.HOUR: "60"
}


class WindDatafeed(BaseDatafeed):
    """万得数据服务接口"""

    def init(self, output: Callable = print) -> bool:
        """初始化"""
        if w.isconnected():
            return True

        data: w.WindData = w.start()
        if data.ErrorCode:
            output(f"Wind数据服务初始化失败，错误码：{data.ErrorCode}")
            return False

        return True

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[BarData]]:
        """查询K线数据"""
        if not w.isconnected():
            self.init(output)

        if req.interval == Interval.DAILY:
            return self.query_daily_bar_history(req, output)
        else:
            return self.query_intraday_bar_history(req, output)

    def query_intraday_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[BarData]]:
        """查询日内K线数据"""
        # 参数转换
        wind_exchange: str = EXCHANGE_MAP[req.exchange]
        wind_symbol: str = f"{req.symbol}.{wind_exchange}"

        fields: List[str] = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amt",
            "oi"
        ]

        wind_interval: str = INTERVAL_MAP[req.interval]
        options: str = f"BarSize={wind_interval}"

        # 发起查询
        error, df = w.wsi(
            codes=wind_symbol,
            fields=fields,
            beginTime=req.start,
            endTime=req.end,
            options=options,
            usedf=True
        )

        # 检查错误
        if error:
            output(f"历史数据查询失败，错误码：{error}")
            return []

        # 补全缺失数值
        df.fillna(value=0, inplace=True)

        # 解析数据
        bars: List[BarData] = []
        for tp in df.itertuples():
            dt: datetime = tp.Index.to_pydatetime()

            # 检查是否有持仓量字段
            if isnan(tp.position):
                open_interest: int = 0
            else:
                open_interest: int = tp.position

            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                interval=req.interval,
                datetime=dt.replace(tzinfo=CHINA_TZ),
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

    def query_daily_bar_history(self, req: HistoryRequest, output: Callable = None) -> Optional[List[BarData]]:
        """查询日K线数据"""
        # 参数转换
        wind_exchange: str = EXCHANGE_MAP[req.exchange]
        wind_symbol: str = f"{req.symbol}.{wind_exchange}"

        fields: List[str] = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amt",
            "oi"
        ]

        # 发起查询
        error, df = w.wsd(
            codes=wind_symbol,
            fields=fields,
            beginTime=req.start,
            endTime=req.end,
            options="",
            usedf=True
        )

        # 检查错误
        if error:
            output(f"历史数据查询失败，错误码：{error}")
            return []

        # 补全缺失数值
        df.fillna(value=0, inplace=True)

        # 解析数据
        bars: List[BarData] = []
        for tp in df.itertuples():
            dt: datetime = datetime.combine(tp.Index, datetime.min.time())

            # 检查是否有持仓量字段
            if isnan(tp.OI):
                open_interest: int = 0
            else:
                open_interest: int = tp.OI

            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                interval=req.interval,
                datetime=dt.replace(tzinfo=CHINA_TZ),
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
