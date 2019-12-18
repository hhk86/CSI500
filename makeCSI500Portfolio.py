import cx_Oracle
import pandas as pd

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

def makePortfolio(fund, tradeDay, hundred=True) -> pd.DataFrame:
    '''
    :return: pd.DataFrame
    :param
    :fund: float, total fund
    :tradeDay: str, the date that we get the CSI500 weight on, yyyymmdd.
    '''
    sql = \
        '''
        SELECT
        ''' + '''
        a.s_con_windcode,
        b.S_INFO_NAME,
        a.open_adjusted,
        a.weight 
        FROM
        aindexcsi500weight a, ASHAREDESCRIPTION b 
        WHERE
        trade_dt = {} 
        AND a.S_CON_WINDCODE = b.S_INFO_WINDCODE  
    '''.format(tradeDay)
    with OracleSql() as oracle:
        df = oracle.query(sql)
    df = lowCaseDfColumns(df)
    df["accurate_shares"] = fund * 0.01 * df["weight"].div(df["open_adjusted"])
    df["shares"] = df["accurate_shares"].apply(lambda x: round(x, -2)).astype(int)
    df = df[df["shares"] > 0]
    if hundred is True:
        df = df[["s_con_windcode", "shares"]]
    else:
        df = df[["s_con_windcode", "accurate_shares"]]
    df.sort_values(by="s_con_windcode", inplace=True)
    df.columns = ["证券代码", "篮子数量"]
    df.to_excel("portfolio.xls", index=False)

    return df

if __name__ == "__main__":
    print(makePortfolio(6000000, "20191218", hundred=True))