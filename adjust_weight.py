import pandas as pd
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


if __name__ == "__main__":
    indexWeight = pd.read_excel('指数权重.xlsx', encoding="gbk", index=None)
    portWeight = pd.read_excel('自有组合权重.xlsx', encoding="gbk",index=None)
    # indexWeight["证券代码"] = indexWeight["证券代码"].apply(lambda s: s[:6])
    portWeight["证券代码"] = portWeight["证券代码"].apply(lambda k : str(k).zfill(6))
    portWeight["证券代码"] = portWeight["证券代码"].apply(lambda s: s + ".SH" if s.startswith('6') else s + ".SZ")

    indexWeight = indexWeight.set_index('证券代码').squeeze()
    portWeight = portWeight.set_index('证券代码').squeeze()
    # indexWeight.to_csv("debug_indexWeight.csv", encoding="gbk")
    # portWeight.to_csv("debug_portWeight.csv", encoding="gbk")

    #  生成共同的index
    unionIndex = indexWeight.index.union(portWeight.index)

    indexWeight = indexWeight.reindex(unionIndex, fill_value = 0.)
    portWeight = portWeight.reindex(unionIndex, fill_value = 0.)


    diffWeight = indexWeight - portWeight
    diffWeight.to_csv("debug.csv", encoding="gbk")
    value = diffWeight * 117034413.67
    portfolio = pd.DataFrame(value)
    portfolio.reset_index(inplace=True)
    portfolio["价格"] = None
    i = 0
    with TsTickData() as tsl:
        for i in range(portfolio.shape[0]):
            tsl_ticker = portfolio.iloc[i,0]
            tsl_ticker = "SH" + tsl_ticker[:6] if tsl_ticker.startswith('6') else "SZ" + tsl_ticker[:6]
            portfolio.iloc[i, 2] = tsl.getHistoricalPrice(ticker=tsl_ticker, date="201912123")
            i += 1
            print(i)
    portfolio.to_csv("debug.csv", encoding="gbk", index=None)
    portfolio = portfolio[portfolio["权重"] != 0]
    portfolio["篮子数量"] = portfolio["权重"].div(portfolio["价格"]).apply(lambda x: round(x, -2)).astype(int)
    portfolio["篮子数量"] = portfolio["权重"].div(portfolio["价格"])
    portfolio_buy = portfolio[portfolio["篮子数量"] > 0]
    portfolio_sell = portfolio[portfolio["篮子数量"] < 0]
    pos_sum = portfolio_buy["权重"].sum()
    neg_sum = portfolio_sell["权重"].sum()
    print(pos_sum, neg_sum, pos_sum + neg_sum)
    portfolio_buy = portfolio_buy[["证券代码", "篮子数量"]]
    portfolio_sell = portfolio_sell[["证券代码", "篮子数量"]]
    portfolio_buy["篮子数量"] = portfolio_buy["篮子数量"].apply(lambda x: round(x / 3, -2)).astype(int)
    portfolio_sell["篮子数量"] = portfolio_sell["篮子数量"].apply(lambda x: round(x / 3, -2)).astype(int)
    portfolio_buy = portfolio_buy[portfolio_buy["篮子数量"] > 0]
    portfolio_sell = portfolio_sell[portfolio_sell["篮子数量"] < 0]
    portfolio_sell["篮子数量"] = - portfolio_sell["篮子数量"]
    portfolio_buy.to_excel("portfolio_buy.xls", encoding="gbk", index=None)
    portfolio_sell.to_excel("portfolio_sell.xls", encoding="gbk", index=None)
    print(portfolio)
