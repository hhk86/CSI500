import numpy as np
from makeCSI500Portfolio import *
from jinja2 import Template
from dateutil.parser import parse as dateparse
import datetime as dt
import sys
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts


class TsTickData(object):

    def __enter__(self):
        if ts.Logined() is False:
            self.__tsLogin()
            return self

    def __tsLogin(self):
        ts.ConnectServer("tsl.tinysoft.com.cn", 443)
        dl = ts.LoginServer("fzzqjyb", "fz123456")

    def __exit__(self, *arg):
        ts.Disconnect()

    def getCurrentPrice(self, ticker):
        ts_sql = ''' 
        setsysparam(pn_stock(),'{}'); 
        rds := rd(6);
        return rds;
        '''.format(ticker)
        fail, value, _ = ts.RemoteExecute(ts_sql, {})
        return value

    def getHistoricalPrice(self, ticker, date):
        ts_sql = ''' 
        setsysparam(pn_stock(), "{0}");
        setsysparam(pn_date(), inttodate({1}));
        return close();
        '''.format(ticker, date)
        fail, value, _ = ts.RemoteExecute(ts_sql, {})
        return value

    def getMarketTable(self, code, start_date, end_date):
        ts_template = Template('''begT:= StrToDate('{{start_date}}');
                                  endT:= StrToDate('{{end_date}}');
                                  setsysparam(pn_cycle(),cy_1s());
                                  setsysparam(pn_rate(),0);
                                  setsysparam(pn_RateDay(),rd_lastday);
                                  r:= select  ["StockID"] as 'ticker', datetimetostr(["date"]) as "time", ["price"]
                                      from markettable datekey begT to endT of "{{code}}" end;
                                  return r;''')
        ts_sql = ts_template.render(start_date=dateparse(start_date).strftime('%Y-%m-%d'),
                                    end_date=dateparse(end_date).strftime('%Y-%m-%d'),
                                    code=code)
        fail, data, _ = ts.RemoteExecute(ts_sql, {})

        def gbk_decode(strlike):
            if isinstance(strlike, (str, bytes)):
                strlike = strlike.decode('gbk')
            return strlike

        def bytes_to_unicode(record):
            return dict(map(lambda s: (gbk_decode(s[0]), gbk_decode(s[1])), record.items()))

        if not fail:
            unicode_data = list(map(bytes_to_unicode, data))
            return pd.DataFrame(unicode_data).set_index(['time', 'ticker'])
        else:
            raise Exception("Error when execute tsl")



def makeup(spot_file: str, makeup_num) -> pd.DataFrame:
    '''
    :param spot_file: 必须是“股份查询-股份组合-全部转成excel（去除千位符）”,即当日组合
    :param makeup_num: 补足到
    :return:
    '''
    spot_df = pd.read_excel(spot_file, encoding="gbk", skiprows=range(0, 4), index_col=None)
    spot_df.drop([spot_df.shape[0] - 1], axis=0, inplace=True)
    spot_df = spot_df[["证券代码", "证券名称", "实时持仓", "买入成本"]]
    spot_df["证券代码"] = spot_df["证券代码"].astype(int)
    spot_df["证券代码"] = spot_df["证券代码"].astype(str)
    spot_df["证券代码"] = spot_df["证券代码"].apply(
        lambda s: s + ".SH" if s.startswith('6') and len(s) == 6 else s.zfill(6) + ".SZ")
    spot_df = spot_df[
        (spot_df["证券代码"] != "511880.SZ") & (spot_df["证券代码"] != "511990.SZ") & (spot_df["证券代码"] != "SZ511660.SZ")]


    spot_df["ticker"] = spot_df["证券代码"].apply(lambda s: "SH" + s[:6] if s.endswith("SH") else "SZ" + s[:6])
    ticker_set = set(spot_df["ticker"])
    current_price_df = pd.DataFrame()
    with TsTickData() as tsl:
        for ticker in ticker_set:
            price = tsl.getCurrentPrice(ticker)
            current_price_df = current_price_df.append(
                pd.DataFrame([[ticker, price], ], columns=["ticker", "current_price"]))
    spot_df = pd.merge(spot_df, current_price_df, on="ticker", how="outer")


    net_spot_sum = spot_df["实时持仓"].mul(spot_df["current_price"]).sum()
    print("现货持仓:", round(net_spot_sum / 1000000, 2), "百万")

    df = makePortfolio(makeup_num, "20191127", hundred=False)
    df = pd.merge(spot_df, df, on="证券代码", how="outer")
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    df["补足数量"] = df["篮子数量"] - df["实时持仓"]
    df["补足数量"] = df["补足数量"].apply(lambda x: round(x, -2))
    df["补足数量"] = np.where(pd.isna(df["实时持仓"]), df["篮子数量"], df["补足数量"])
    df["实时持仓"] = np.where(pd.isna(df["实时持仓"]), 0, df["实时持仓"])
    df["补足数量"] = np.where(pd.isna(df["篮子数量"]), - df["实时持仓"], df["补足数量"])
    df["篮子数量"] = np.where(pd.isna(df["篮子数量"]), 0, df["篮子数量"])
    df["补足数量"] = df["补足数量"].apply(lambda x: int(round(x, -2)))
    df["补足比率"] = df["补足数量"].div(df["篮子数量"]).apply(lambda x: str(round(100 * x)) + '%' if x < 1000000 and x > -1000000 else np.nan)

    print("补足到:", round(makeup_num / 1000000, 2), "百万")
    buy_df = df[df["补足数量"] > 0]
    print("补足金额:", round(buy_df["补足数量"].mul(buy_df["current_price"]).sum() / 1000000, 2), "百万")
    print("补足股票个数:", buy_df.shape[0], "个")
    print("补足后现货:", round(net_spot_sum / 1000000 + buy_df["补足数量"].mul(buy_df["current_price"]).sum() / 1000000, 2), "百万")
    buy_df.index = range(buy_df.shape[0])
    buy_df = buy_df[["证券代码", "补足数量"]]
    buy_df.columns = ["证券代码", "篮子数量"]
    buy_df.to_excel("补足-买.xls", encoding="gbk", index=None)
    sell_df = df[df["补足数量"] < 0]
    print("未补齐金额:",  round(- sell_df["补足数量"].mul(sell_df["current_price"]).sum()/ 1000000, 2), "百万")
    print('保存篮子到：“补足-买.xls”')



if __name__ == "__main__":
    makeup(spot_file="current_pos.xlsx", makeup_num=120000000)
