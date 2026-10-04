import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest


# WindPy 不参与测试。w.isconnected 保持为真，避免 w.start 登录终端。
class _Wind:
    def __init__(self) -> None:
        self.connected: bool = True
        self.wsi_calls: list[dict[str, object]] = []
        self.wsd_calls: list[dict[str, object]] = []
        self.intraday: pd.DataFrame = pd.DataFrame()
        self.daily: pd.DataFrame = pd.DataFrame()

    def isconnected(self) -> bool:
        return self.connected

    def start(self) -> object:
        raise AssertionError("w.start")

    def wsi(
        self,
        codes: str,
        fields: list[str],
        beginTime: datetime,
        endTime: datetime,
        options: str,
        usedf: bool,
    ) -> tuple[int, pd.DataFrame]:
        self.wsi_calls.append({
            "codes": codes,
            "fields": fields,
            "beginTime": beginTime,
            "endTime": endTime,
            "options": options,
            "usedf": usedf,
        })
        return 0, self.intraday

    def wsd(
        self,
        codes: str,
        fields: list[str],
        beginTime: datetime,
        endTime: datetime,
        options: str,
        usedf: bool,
    ) -> tuple[int, pd.DataFrame]:
        self.wsd_calls.append({
            "codes": codes,
            "fields": fields,
            "beginTime": beginTime,
            "endTime": endTime,
            "options": options,
            "usedf": usedf,
        })
        return 0, self.daily

    def wsq(self, *args: object, **kwargs: object) -> None:
        return None


_wind: _Wind = _Wind()
_windpy: ModuleType = ModuleType("WindPy")
_windpy.w = _wind  # type: ignore[attr-defined]
sys.modules["WindPy"] = _windpy

from vnpy_wind.wind_datafeed import CHINA_TZ, WindDatafeed  # noqa: E402


_INTRADAY: pd.DataFrame = pd.DataFrame(
    {
        "open": [100.5],
        "high": [110.0],
        "low": [90.25],
        "close": [105.0],
        "volume": [12.0],
        "amount": [1300.0],
        "position": [33.0],
    },
    index=pd.to_datetime(["2024-01-15 10:00:00"]),
)
_DAILY: pd.DataFrame = pd.DataFrame(
    {
        "OPEN": [100.5],
        "HIGH": [110.0],
        "LOW": [90.25],
        "CLOSE": [105.0],
        "VOLUME": [12.0],
        "AMT": [1300.0],
        "OI": [33.0],
    },
    index=[datetime(2024, 1, 15).date()],
)


def _request(symbol: str, exchange: Exchange, interval: Interval) -> HistoryRequest:
    return HistoryRequest(
        symbol=symbol,
        exchange=exchange,
        start=datetime(2024, 1, 15, 9, 0),
        end=datetime(2024, 1, 15, 15, 0),
        interval=interval,
    )


def _assert_bar(bar: BarData, symbol: str, exchange: Exchange, interval: Interval, bar_dt: datetime) -> None:
    assert bar.symbol == symbol
    assert bar.exchange == exchange
    assert bar.interval == interval
    assert bar.datetime == bar_dt
    assert bar.open_price == 100.5
    assert bar.high_price == 110.0
    assert bar.low_price == 90.25
    assert bar.close_price == 105.0
    assert bar.volume == 12.0


@pytest.mark.parametrize(
    ("symbol", "exchange", "wind_symbol"),
    [
        ("600000", Exchange.SSE, "600000.SH"),
        ("000001", Exchange.SZSE, "000001.SZ"),
        ("IF2410", Exchange.CFFEX, "IF2410.CFE"),
        ("rb2410", Exchange.SHFE, "rb2410.SHF"),
        ("TA501", Exchange.CZCE, "TA501.CZC"),
        ("i2501", Exchange.DCE, "i2501.DCE"),
    ],
)
def test_intraday_exchange_and_bar(symbol: str, exchange: Exchange, wind_symbol: str) -> None:
    assert Path(sys.modules["vnpy_wind.wind_datafeed"].__file__ or "").resolve().is_relative_to(
        Path(__file__).resolve().parents[1]
    )

    _wind.wsi_calls.clear()
    _wind.intraday = _INTRADAY.copy()
    logs: list[str] = []
    bars: list[BarData] = WindDatafeed().query_bar_history(
        _request(symbol, exchange, Interval.MINUTE),
        output=logs.append,
    )

    assert logs == []
    assert _wind.wsi_calls[0]["codes"] == wind_symbol
    assert _wind.wsi_calls[0]["options"] == "BarSize=1"
    assert len(bars) == 1
    _assert_bar(
        bars[0],
        symbol,
        exchange,
        Interval.MINUTE,
        datetime(2024, 1, 15, 10, 0, tzinfo=CHINA_TZ),
    )


def test_hour_bar_size() -> None:
    _wind.wsi_calls.clear()
    _wind.intraday = _INTRADAY.copy()
    logs: list[str] = []
    bars: list[BarData] = WindDatafeed().query_bar_history(
        _request("rb2410", Exchange.SHFE, Interval.HOUR),
        output=logs.append,
    )

    assert logs == []
    assert _wind.wsi_calls[0]["codes"] == "rb2410.SHF"
    assert _wind.wsi_calls[0]["options"] == "BarSize=60"
    assert len(bars) == 1
    _assert_bar(
        bars[0],
        "rb2410",
        Exchange.SHFE,
        Interval.HOUR,
        datetime(2024, 1, 15, 10, 0, tzinfo=CHINA_TZ),
    )


def test_daily_exchange_and_bar() -> None:
    _wind.wsd_calls.clear()
    _wind.daily = _DAILY.copy()
    logs: list[str] = []
    bars: list[BarData] = WindDatafeed().query_bar_history(
        _request("rb2410", Exchange.SHFE, Interval.DAILY),
        output=logs.append,
    )

    assert logs == []
    assert _wind.wsd_calls[0]["codes"] == "rb2410.SHF"
    assert len(bars) == 1
    _assert_bar(
        bars[0],
        "rb2410",
        Exchange.SHFE,
        Interval.DAILY,
        datetime(2024, 1, 15, tzinfo=CHINA_TZ),
    )
