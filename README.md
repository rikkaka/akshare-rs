将[akshare](https://github.com/akfamily/akshare)中的常用API翻译为rust版本。使用[polars](https://github.com/pola-rs/polars)处理数据框。

目前已翻译API：
- [stock_zh_a_hist](https://akshare.akfamily.xyz/data/stock/stock.html#id21) 单次返回指定沪深京 A 股上市公司、指定周期和指定日期间的历史行情日频率数据
- [stock_zh_a_spot_em](https://www.akshare.xyz/data/stock/stock.html#id12) 单次返回所有沪深京 A 股上市公司的实时行情数据
- [stock_sse_summary](https://akshare.akfamily.xyz/data/stock/stock.html#id2) 单次返回上证最近交易日的股票数据总貌