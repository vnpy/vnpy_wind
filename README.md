# VeighNa框架的万得Wind数据服务接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.5-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.8|3.9|3.10|3.11|3.12-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

基于万得WindPy接口开发，支持以下中国金融市场的K线数据：

* 期货：
  * CFFEX：中国金融期货交易所
  * SHFE：上海期货交易所
  * DCE：大连商品交易所
  * CZCE：郑州商品交易所
  * INE：国际能源交易中心
  * GFEX：广州期货交易所
* 股票：
  * SSE：上海证券交易所
  * SZSE：深圳证券交易所

注意：需要购买[Wind金融终端](https://www.wind.com.cn)，并安装好Python版本数据接口（WindPy）。


## 安装

安装环境推荐基于3.9.0版本以上的【[**VeighNa Studio**](https://www.vnpy.com)】。

直接使用pip命令：

```
pip install vnpy_wind
```


或者下载源代码后，解压后在cmd中运行：

```
pip install .
```
