import cx_Oracle
import pandas as pd

pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)


class OracleSql(object):
    '''
    Query data from database
    '''

    def __init__(self):
        '''
        Initialize database
        '''
        self.host, self.oracle_port = '18.210.64.72', '1521'
        self.db, self.current_schema = 'tdb', 'wind'
        self.user, self.pwd = 'reader', 'reader'

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
            print('连接oracle数据库')
        except Exception:
            print('不能连接oracle数据库')
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


if __name__ == '__main__':
    sqlLine_1 = ("\n",
                 "    SELECT\n",
                 "	a.trade_dt,\n",
                 "	a.s_info_windcode,\n",
                 "	b.S_INFO_NAME,\n",
                 "	a.s_con_windcode,\n",
                 "	a.tot_shr,\n",
                 "	a.free_shr_ratio,\n",
                 "	a.shr_calculation,\n",
                 "	a.closevalue,\n",
                 "	a.open_adjusted,\n",
                 "	a.tot_mv,\n",
                 "	a.mv_calculation,\n",
                 "	a.weight \n",
                 "FROM\n",
                 "	aindexcsi500weight a，ASHAREDESCRIPTION b \n",
                 "WHERE\n",
                 "	trade_dt = 20190710 \n",
                 "	AND a.S_CON_WINDCODE = b.S_INFO_WINDCODE\n",
                 "    ")

    sql_1 = ' '.join(sqlLine_1)
    sqlLine_2 = ("\n",
                 "        SELECT\n",
                 "            a.s_info_windcode,\n",
                 "            b.Industriesname \n",
                 "        FROM\n",
                 "            AShareIndustriesClassCITICS a,\n",
                 "            AShareIndustriesCode b \n",
                 "        WHERE\n",
                 "            substr( a.citics_ind_code, 1, 4 ) = substr( b.IndustriesCode, 1, 4 ) \n",
                 "            AND b.levelnum = '2' \n",
                 "            AND a.cur_sign = '1' \n",
                 "        ORDER BY\n",
                 "            1\n",
                 "        ")
    sql_2 = ' '.join(sqlLine_2)
    sqlLine_3 = ("\n",
                 "        SELECT\n",
                 "            a.s_info_windcode,\n",
                 "            b.Industriesname\n",
                 "        FROM\n",
                 "            AShareIndustriesClassCITICS a,\n",
                 "            AShareIndustriesCode b \n",
                 "        WHERE\n",
                 "			 substr( b.IndustriesCode, 1, 4 )  = 'b10m'\n",
                 "            AND substr( a.citics_ind_code, 1, 6 ) = substr( b.IndustriesCode, 1, 6 ) \n",
                 "            AND b.levelnum = '3' \n",
                 "            AND a.cur_sign = '1' \n",
                 "        ORDER BY\n",
                 "            1\n",
                 "        ")
    sql_3 = ' '.join(sqlLine_3)

    with OracleSql() as oracle:
        weightData = oracle.query(sql_1)
        industryCodeData_1 = oracle.query(sql_2)
        industryCodeData_2 = oracle.query(sql_3)

    weightData.columns = [s.lower() for s in weightData.columns]
    weightData.drop(columns=["s_info_windcode", "tot_mv", "mv_calculation"], inplace=True)
    industryCodeData_1.columns = [s.lower() for s in industryCodeData_1]
    industryCodeData_2.columns = [s.lower() for s in industryCodeData_2]
    industryCodeData = pd.concat(
        [industryCodeData_1[industryCodeData_1["industriesname"] != "非银行金融"], industryCodeData_2])
    industryCodeData.sort_values(by=["s_info_windcode", ])
    industryCodeData.index = range(len(industryCodeData))

    rawStockData = pd.merge(weightData, industryCodeData, left_on="s_con_windcode", right_on="s_info_windcode")
    rawStockData = rawStockData[["s_con_windcode", "s_info_name", "industriesname", "weight", "closevalue"]]
    rawStockData.columns = ["stockCode", "stockName","industriesName", "weight", "closeValue"]
    industryWeight = rawStockData.groupby("industriesName")["weight"].sum().squeeze().reset_index()
    stockData = rawStockData[rawStockData["closeValue"] >= 3]
    screenedIndustryWeight = stockData.groupby("industriesName")["weight"].sum().squeeze().reset_index()
    screenedIndustryWeight = pd.merge(screenedIndustryWeight, industryWeight, on="industriesName")
    screenedIndustryWeight["scale"] = screenedIndustryWeight["weight_y"] / screenedIndustryWeight["weight_x"]
    stockData = pd.merge(stockData, screenedIndustryWeight[["industriesName", "scale"]], on="industriesName")
    stockData["adjustedWeight"] = stockData["weight"] * stockData["scale"]
    adjustedIndustryWeight = stockData[["stockCode", "stockName", "industriesName", "adjustedWeight"]]
    print("\n调整权重表")
    print(adjustedIndustryWeight)
    print("\n"* 5 + "个股数据表")
    print(stockData)
    # print(pd.merge(adjustedIndustryWeight.groupby("industriesName").sum(), industryWeight.groupby("industriesName").sum(), left_index=True, right_index=True))
    stockData.to_csv("stockData.csv", encoding = "gbk")




