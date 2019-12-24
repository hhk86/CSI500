import pandas as pd
import sys
import datetime as dt
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts
from makeCSI500Portfolio import *

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


def adjust_weight(position_file: str, prev_date: str) -> None:
    # Position file should be 股份查询-股份组合，存成xlsx并去除千位符
    # Prev_date is the last trading day.
    port = pd.read_excel(position_file, encoding="gbk", skiprows=range(0, 4), index_col=None)
    port.index = list(range(port.shape[0]))
    port.drop([port.shape[0] - 1], axis=0, inplace=True)
    port = port[["证券代码", "证券名称", "股份余额"]]   # 股份余额是按昨日余额结转的，实时持仓是当前余额
    port["证券代码"] = port["证券代码"].apply(lambda k: str(int(k)).zfill(6))
    port["证券代码"] = port["证券代码"].apply(lambda s: "SH" + s if s.startswith('6') else "SZ" + s)
    port = port[(port["证券代码"] != "SZ511880") & (port["证券代码"] != "SZ511990") \
                          & (port["证券代码"] != "SZ511660") & (port["股份余额"] != 0)]  # Delete money fund and debts
    index = makePortfolio(10000000, tradeDay=prev_date, hundred=False)
    index["证券代码"] = index["证券代码"].apply(lambda s: s[-2:] + s[:6])
    port = port.set_index('证券代码').squeeze()
    index = index.set_index('证券代码').squeeze()
    unionIndex = index.index.union(port.index)
    unionPrice = pd.DataFrame()
    with TsTickData() as tsl:
        for tsl_ticker in list(unionIndex):
            price = tsl.getHistoricalPrice(ticker=tsl_ticker, date=prev_date)
            unionPrice = unionPrice.append([[tsl_ticker, price],])
    unionPrice.columns = ["证券代码", "前收价格"]
    port = pd.merge(port, unionPrice, left_index=True, right_on="证券代码")
    index = pd.merge(index, unionPrice, left_index=True, right_on="证券代码")
    port["持仓金额"] = port["股份余额"].mul(port["前收价格"])
    port_sum = port["持仓金额"].sum()
    port["权重"] = port["持仓金额"] / port_sum
    index["篮子金额"] = index["篮子数量"].mul(index["前收价格"])
    index_sum = index["篮子金额"].sum()
    index["权重"] = index["篮子金额"] / index_sum
    portWeight = port[["证券代码", "权重"]].set_index('证券代码').squeeze()
    indexWeight = index[["证券代码", "权重"]].set_index('证券代码').squeeze()
    unionIndex = indexWeight.index.union(portWeight.index)
    indexWeight = indexWeight.reindex(unionIndex, fill_value = 0.)
    portWeight = portWeight.reindex(unionIndex, fill_value = 0.)
    indexWeight.to_csv("debug.csv", encoding="gbk")
    portWeight.to_csv("debug2.csv", encoding="gbk")
    diffWeight = indexWeight - portWeight
    value = diffWeight * port_sum
    portfolio = pd.DataFrame(value)
    portfolio.reset_index(inplace=True)
    portfolio.columns = ["证券代码", "篮子金额"]
    portfolio = pd.merge(portfolio, unionPrice, on="证券代码")
    portfolio["篮子数量"] = portfolio["篮子金额"].div(portfolio["前收价格"])
    portfolio["证券代码"] = portfolio["证券代码"].apply(lambda s: s[3:] + '.' + s[:2])
    portfolio_buy = portfolio[portfolio["篮子数量"] > 0]
    portfolio_sell = portfolio[portfolio["篮子数量"] < 0]
    pos_sum = portfolio_buy["篮子金额"].sum()
    neg_sum = portfolio_sell["篮子金额"].sum()
    print("买入", round(pos_sum / 10000, 2), "万， 卖出", round(neg_sum / 10000, 2), "万")
    numOfPortfolio = int(input("请输入要分成的篮子数量："))
    portfolio_buy = portfolio_buy[["证券代码", "篮子数量"]]
    portfolio_sell = portfolio_sell[["证券代码", "篮子数量"]]
    portfolio_buy["篮子数量"] = portfolio_buy["篮子数量"].apply(lambda x: round(x / numOfPortfolio, -2)).astype(int)
    portfolio_sell["篮子数量"] = portfolio_sell["篮子数量"].apply(lambda x: round(x / numOfPortfolio, -2)).astype(int)
    portfolio_buy = portfolio_buy[portfolio_buy["篮子数量"] > 0]
    portfolio_sell = portfolio_sell[portfolio_sell["篮子数量"] < 0]
    portfolio_sell["篮子数量"] = - portfolio_sell["篮子数量"]
    portfolio_buy.to_excel("portfolio_buy.xls", encoding="gbk", index=None)
    portfolio_sell.to_excel("portfolio_sell.xls", encoding="gbk", index=None)
    print(portfolio_buy)
    print(portfolio_sell)





if __name__ == "__main__":
    adjust_weight("position_1224.xlsx", prev_date="20191223")

