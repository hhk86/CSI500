import cx_Oracle
import pandas as pd
import matplotlib.pyplot as plt
import os
import shutil

# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)


class OracleSql(object):
    '''
    Query data from database
    '''

    def __init__(self, pt=False):
        '''
        Initialize database
        '''
        self.host, self.oracle_port = '18.210.64.72', '1521'
        self.db, self.current_schema = 'tdb', 'wind'
        self.user, self.pwd = 'reader', 'reader'
        self.pt = pt

    def __enter__(self):
        '''
        Connect to database
        :return: self
        '''
        self.conn = self.__connect_to_oracle()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def __connect_to_oracle(self):
        '''
        Connect to database
        :return: connection
        '''
        dsn = self.host + ':' + self.oracle_port + '/' + self.db
        try:
            connection = cx_Oracle.connect(self.user, self.pwd, dsn, encoding="UTF-8", nencoding="UTF-8")
            connection.current_schema = self.current_schema
            if self.pt is True:
                print('Connected to Oracle database successful!')
        except Exception:
            print('Failed on connecting to Oracle database!')
            connection = None
        return connection

    def query(self, sql: str) -> pd.DataFrame:
        '''
        Query data
        '''
        return pd.read_sql(sql, self.conn)

    def execute(self, sql: str):
        '''
        Execute SQL scripts, including inserting and updating

        '''
        self.conn.cursor().execute(sql)
        self.conn.commit()


def lowCaseDfColumns(df: pd.DataFrame) -> pd.DataFrame:
    '''
    :param df: pd.DataFrame
    :return: pd.DataFrame
    '''

    df.columns = [s.lower() for s in df.columns]
    return df


def backtest(startDate: str, endDate: str, filter_param={"minPrice": 3, "maxPrice": 10000}, plot=True):
    '''
    :param startDate: str, included in the interval
    :param endDate: str, included in the interval
    :param filter_param: dict, dictionary of filter parameters
    :param plot: bool, whether to plot the cumulative return
    :return: pd.DataFrame, table of returns of CSI500 and sample
    '''

    CSI500Returns = getCSI500Returns(startDate, endDate)
    sampleReturns = getSampleReturns(startDate, endDate, filter_param=filter_param)
    returns_df = pd.concat([CSI500Returns,sampleReturns], axis=1)
    returns_df.columns = ["CSI500DailyReturn", "CSI500CumulativeReturn","sampleDailyReturn", "sampleCumulativeReturn"]
    period = startDate + '_' + endDate
    try:
        shutil.rmtree(period)
    except:
        pass
    path = period + '\\'
    os.makedirs(path)
    returns_df.to_csv(path + "CSI500_vs_sample_" + period + ".csv")
    if plot is True:
        plotResult(returns_df, period, path)


def getTradingDays(startDate: str, endDate: str) -> list:
    sql = \
        '''
        SELECT
        ''' + '''
	TRADE_DAYS 
    FROM
        asharecalendar 
    WHERE
        S_INFO_EXCHMARKET = 'SSE' 
        AND trade_days BETWEEN {} AND {}
    '''.format(startDate, endDate)
    with OracleSql() as oracle:
        tradingDays = oracle.query(sql)
    return list(tradingDays.TRADE_DAYS)


def getCSI500Returns(startDate: str, endDate: str) -> pd.DataFrame:
    '''
    :param startDate: str, included in the interval
    :param endDate: str, included in the interval
    :return: pd.DataFrame, table of returns of CSI500
    '''
    sql = \
        '''
        SELECT
        ''' + '''
	TRADE_DT, S_DQ_PCTCHANGE 
    FROM
        AIndexEODPrices 
    WHERE
        S_INFO_WINDCODE = 'h00905.CSI' 
        AND Trade_dt between {}
        AND {}
    ORDER BY TRADE_DT
    '''.format(startDate, endDate)

    with OracleSql() as oracle:
        CSI500Returns = oracle.query(sql)
    CSI500Returns = lowCaseDfColumns(CSI500Returns)
    CSI500Returns.set_index("trade_dt", inplace=True)
    CSI500Returns.columns = ["return", ]
    CSI500Returns /= 100
    CSI500Returns["cumprod"] = (CSI500Returns["return"] + 1).cumprod()
    return CSI500Returns


def getSampleReturns(startDate: str, endDate: str, filter_param={"minPrice": 3, "maxPrice": 10000}) -> pd.DataFrame:
    '''
    :param startDate: str, included in the interval
    :param endDate: str, included in the interval
    :return: pd.DataFrame, table of returns of sample
    '''
    global useLocalData
    print("start backtesting")
    if useLocalData is True:
        dailyReturnOfAShare = pd.read_csv("adjustedDailyReturnOfAShare.csv")
        dailyReturnOfAShare = lowCaseDfColumns(dailyReturnOfAShare)
        dailyReturnOfAShare.trade_dt = dailyReturnOfAShare.trade_dt.astype(str)
        sampleReturn_array = pd.Series()
        stat = pd.DataFrame(columns = ["NumOfConstituent", "SumOfWeight"])
        for date in getTradingDays(startDate, endDate):
            oneDayReturnOfAShare = dailyReturnOfAShare[dailyReturnOfAShare["trade_dt"] == date]
            oneDaySample = getDailySample(date, filter_param=filter_param)
            oneDaySample = pd.merge(oneDaySample, oneDayReturnOfAShare, left_index=True,
                                    right_on="s_info_windcode")
            oneDaysampleReturn = oneDaySample.adjustedWeight.mul(oneDaySample.dailyreturn + 1).sum() / 100 - 1
            sampleReturn_array[date] = oneDaysampleReturn
            stat.loc[date,:] = [len(oneDaySample), oneDaySample.weight.sum()]
            print(date)
        # print(stat)
        # stat.to_csv("stat.csv")
        sampleReturns = pd.DataFrame(sampleReturn_array, columns=["return", ])
        sampleReturns["cumprod"] = (sampleReturns["return"] + 1).cumprod()

    return sampleReturns


def getDailySample(tradeDay: str, filter_param={"minPrice": 3, "maxPrice": 10000}) -> pd.DataFrame:
    '''

    :param tradeDay: str, "yyyymmdd"
    :param filter_param: dict, dictionary of filter parameters
    :return: pd.DataFrame
    '''

    daily500 = getDaily500Stock(tradeDay)
    daily500IndustryWeight = daily500.groupby("industriesName")["weight"].sum()
    dailySample = daily500[daily500["closeValue"] >= filter_param["minPrice"]]
    dailySampleIndustryWeight = dailySample.groupby("industriesName")["weight"].sum()
    scale = pd.DataFrame(daily500IndustryWeight.div(dailySampleIndustryWeight))
    scale.columns = ["scale", ]
    dailySample = pd.merge(dailySample, scale, left_on="industriesName", right_index=True)
    dailySample["adjustedWeight"] = dailySample["weight"].mul(dailySample["scale"])
    return dailySample


def getDaily500Stock(tradeDay: str) -> pd.DataFrame:
    '''
    :param tradeday: str, "yyyymmdd"
    :return: pd.DataFrame
    '''

    daily500Data = getDaily500Data(tradeDay)
    industryCode = getIndustryCode()
    daily500Stock = pd.merge(daily500Data, industryCode, left_index=True, right_index=True)
    daily500Stock = daily500Stock[["s_info_name", "industriesname", "weight", "closevalue"]]
    daily500Stock.columns = ["stockName", "industriesName", "weight", "closeValue"]
    return daily500Stock


def getDaily500Data(tradeDay: str) -> pd.DataFrame:
    '''
    :param tradday: str, "yyyymmdd"
    :param useOracle: bool, get daily data from local or Oracle
    :return: pd.DataFrame
    '''
    global useLocalData
    global CSI500WeightData
    if useLocalData is True:
        daily500Data = CSI500WeightData[CSI500WeightData["trade_dt"] == tradeDay]
        daily500Data.set_index(["s_con_windcode", ], inplace=True)
    else:
        sql = \
            '''
            SELECT
            ''' + '''
            a.trade_dt,
            a.s_con_windcode,
            b.S_INFO_NAME,
            a.tot_shr,
            a.free_shr_ratio,
            a.shr_calculation,
            a.closevalue,
            a.open_adjusted,
            a.weight 
            FROM
            aindexcsi500weight a, ASHAREDESCRIPTION b 
            WHERE
            trade_dt = {} 
            AND a.S_CON_WINDCODE = b.S_INFO_WINDCODE  
        '''.format(tradeDay)
        with OracleSql() as oracle:
            daily500Data = oracle.query(sql)
        daily500Data = lowCaseDfColumns(daily500Data)
        daily500Data.set_index(["s_con_windcode", ], inplace=True)
    return daily500Data


def getIndustryCode() -> pd.DataFrame:
    '''
    Get CITICS industry code
    :return: pd.DataFrame
    '''

    sql_1 = \
        '''
        SELECT
        ''' + '''
        a.s_info_windcode,
        b.Industriesname
        FROM
        AShareIndustriesClassCITICS a,
        AShareIndustriesCode b
        WHERE
        substr( a.citics_ind_code, 1, 4 ) = substr( b.IndustriesCode, 1, 4 )
        AND b.levelnum = '2'
        AND a.cur_sign = '1'
        ORDER BY
        1
        '''
    sql_2 = \
        '''
        SELECT
        ''' + '''
        a.s_info_windcode,
        b.Industriesname
        FROM
        AShareIndustriesClassCITICS a,
        AShareIndustriesCode b
        WHERE
        substr( b.IndustriesCode, 1, 4 )  = 'b10m'
        AND substr( a.citics_ind_code, 1, 6 ) = substr( b.IndustriesCode, 1, 6 )
        AND b.levelnum = '3'
        AND a.cur_sign = '1'
        ORDER BY
        1
        '''
    with OracleSql() as oracle:
        industryCode_1 = oracle.query(sql_1)
        industryCode_2 = oracle.query(sql_2)
    industryCode_1 = lowCaseDfColumns(industryCode_1)
    industryCode_2 = lowCaseDfColumns(industryCode_2)
    industryCode = pd.concat(
        [industryCode_1[industryCode_1["industriesname"] != "非银行金融"], industryCode_2])
    industryCode.columns = ["windcode", "industriesname"]
    industryCode.set_index(["windcode", ], drop=True, inplace=True)
    return industryCode


def getDateTickLabel(tradeDay_list: list, numOfTicks = 5) -> (list, list):
    '''
    Give a trade day list and return xticks and xticklabels
    :param tradeDay_ls: list
    :return: (xticks, xticklabels)
    '''
    xticks = list()
    N = len(tradeDay_list)
    interval = N // numOfTicks
    for i in range(N):
        if i * interval < N:
            xticks.append(tradeDay_list[i * interval])
    xticklabels = xticks
    return (xticks, xticklabels)


def plotResult(returns_df: pd.DataFrame, period: str, path: str):
    '''
    Plot backtesting result
    :param returns_df: pd.DataFrame, df of daily returns and cumulative returns.
    '''

    plt.plot(returns_df["CSI500CumulativeReturn"])
    plt.plot(returns_df["sampleCumulativeReturn"])
    xticks, xticklabels = getDateTickLabel(list(returns_df.index))
    plt.xticks(xticks, xticklabels)
    plt.legend(["CSI500", "sample"])
    plt.title("P&L")
    plt.savefig(path + "cumulative_return_" + period + ".png")
    plt.show()
    plt.close()

    plt.plot(returns_df["CSI500DailyReturn"])
    plt.plot(returns_df["sampleDailyReturn"])
    xticks, xticklabels = getDateTickLabel(list(returns_df.index))
    plt.xticks(xticks, xticklabels)
    plt.legend(["CSI500", "sample"])
    plt.title("Daily Return")
    plt.savefig(path + "daily_return_" + period + ".png")
    plt.show()
    plt.close()

    plt.plot(returns_df.sampleCumulativeReturn - returns_df.CSI500CumulativeReturn)
    xticks, xticklabels = getDateTickLabel(list(returns_df.index))
    plt.xticks(xticks, xticklabels)
    plt.title("Difference of P&L : sample - CSI500")
    plt.savefig(path + "cumulative_diff_" + period + ".png")
    plt.show()
    plt.close()

    plt.plot(returns_df.sampleDailyReturn - returns_df.CSI500DailyReturn)
    xticks, xticklabels = getDateTickLabel(list(returns_df.index))
    plt.xticks(xticks, xticklabels)
    plt.title("Difference of daily return : sample - CSI500")
    plt.savefig(path + "daily_return_diff_" + period + ".png")
    plt.show()
    plt.close()

if __name__ == "__main__":
    useLocalData = True
    CSI500WeightData = pd.read_csv("CSI500WeightData.csv")
    CSI500WeightData = lowCaseDfColumns(CSI500WeightData)
    CSI500WeightData.trade_dt = CSI500WeightData.trade_dt.astype(str)
    # sampleReturns = backtest("20190101", "20190710")
    sampleReturns = backtest("20190615", "20190710")


