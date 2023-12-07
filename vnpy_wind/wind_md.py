from typing import Dict
from copy import copy
from datetime import datetime

from WindPy import w

from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import TickData, SubscribeRequest
from vnpy.trader.utility import ZoneInfo
from vnpy.trader.constant import Exchange


CHINA_TZ = ZoneInfo("Asia/Shanghai")


EXCHANGE_WIND_VT: Dict[str, Exchange] = {
    "SH": Exchange.SSE,
    "SZ": Exchange.SZSE,
    "CFE": Exchange.CFFEX,
    "SHF": Exchange.SHFE,
    "CZC": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE,
    "GFE": Exchange.GFEX
}
EXCHANGE_VT_WIND = {v: k for k, v in EXCHANGE_WIND_VT.items()}


class WindMdApi:
    """Wind行情API实例"""

    wsq_param_map = {}
    wsq_param_map["rt_last"] = "last_price"
    wsq_param_map["rt_last_vol"] = "volume"
    wsq_param_map["rt_oi"] = "open_interest"
    wsq_param_map["rt_open"] = "open_price"
    wsq_param_map["rt_high"] = "high_price"
    wsq_param_map["rt_low"] = "low_price"
    wsq_param_map["rt_pre_close"] = "pre_close"
    wsq_param_map["rt_high_limit"] = "upper_limit"
    wsq_param_map["rt_low_limit"] = "lower_limit"
    wsq_param_map["rt_bid1"] = "bid_price_1"
    wsq_param_map["rt_bid2"] = "bid_price_2"
    wsq_param_map["rt_bid3"] = "bid_price_3"
    wsq_param_map["rt_bid4"] = "bid_price_4"
    wsq_param_map["rt_bid5"] = "bid_price_5"
    wsq_param_map["rt_ask1"] = "ask_price_1"
    wsq_param_map["rt_ask2"] = "ask_price_2"
    wsq_param_map["rt_ask3"] = "ask_price_3"
    wsq_param_map["rt_ask4"] = "ask_price_4"
    wsq_param_map["rt_ask5"] = "ask_price_5"
    wsq_param_map["rt_bsize1"] = "bid_volume_1"
    wsq_param_map["rt_bsize2"] = "bid_volume_2"
    wsq_param_map["rt_bsize3"] = "bid_volume_3"
    wsq_param_map["rt_bsize4"] = "bid_volume_4"
    wsq_param_map["rt_bsize5"] = "bid_volume_5"
    wsq_param_map["rt_asize1"] = "ask_volume_1"
    wsq_param_map["rt_asize2"] = "ask_volume_2"
    wsq_param_map["rt_asize3"] = "ask_volume_3"
    wsq_param_map["rt_asize4"] = "ask_volume_4"
    wsq_param_map["rt_asize5"] = "ask_volume_5"

    def __init__(self, gateway: BaseGateway):
        """"""
        self.gateway: BaseGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.connected: bool = False
        self.ticks: Dict[str, TickData] = {}
        self.subscribed: Dict[str, SubscribeRequest] = {}

    def connect(self):
        """"""
        data = w.start()
        if not data.ErrorCode:
            self.gateway.write_log("Wind行情接口连接成功")
        else:
            self.gateway.write_log("Wind行情接口连接失败")
            return

        for req in self.subscribed.values():
            self.subscribe(req)

    def subscribe(self, req: SubscribeRequest):
        """"""
        self.subscribed[req.vt_symbol] = req

        wind_exchange = EXCHANGE_VT_WIND[req.exchange]
        wind_symbol = f"{req.symbol}.{wind_exchange}"

        # 这里额外做一次检查，避免w丢失初始化状态
        if not w.isconnected():
            w.start()

        w.wsq(
            wind_symbol, list(self.wsq_param_map.keys()), func=self.wsq_callback
        )

        self.ticks[wind_symbol] = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            datetime=datetime.now(),
            gateway_name=self.gateway_name
        )

    def wsq_callback(self, data):
        """"""
        wind_symbol = data.Codes[0]
        tick = self.ticks[wind_symbol]

        dt = data.Times[0]
        tick.datetime = dt.replace(tzinfo=CHINA_TZ)

        for n, field in enumerate(data.Fields):
            field = field.lower()
            key = self.wsq_param_map[field]
            value = data.Data[n][0]
            setattr(tick, key, value)

        self.gateway.on_tick(copy(tick))
