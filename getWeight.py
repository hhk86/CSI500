import pandas as pd
import cx_Oracle

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


def getConstituent():
    sql = '''
    SELECT
        S_CON_WINDCODE
    FROM
        AIndexMembers 
    WHERE
        S_INFO_WINDCODE = '{0}'
        AND CUR_SIGN = 1
        '''.format("000905.SH")
    with OracleSql() as oracle:
        data =  oracle.query(sql)
    data.columns = ["S_INFO_WINDCODE", ]
    return data


def getIndustry() -> pd.DataFrame:
    '''
    查询某一日期区间内（包括查询初始日和结束日）每天股票的中信行业一级分类和二级分类。
    AShareIndustriesClassCITIC表中[ENTRY_DT, REMOVE_DT]是闭区间，与AShareST中的左闭右开区间不同，因此使用makeStkName逻辑。
    :param start_date: str, 初始日期，"YYYYMMDD"
    :param end_date: str, 结束日期，"YYYYMMDD"
    :return: pd.DataFrame, columns = [tradeday - str，ticker - str，name - str, citic_industry_L1 - str, citic_industry_L2 - str]
    '''

    sql = \
        '''
        SELECT
            a.S_INFO_WINDCODE,
            b.INDUSTRIESNAME
        FROM
            ASHAREINDUSTRIESCLASSCITICS a,
            ASHAREINDUSTRIESCODE b 
        WHERE
            substr(a.CITICS_IND_CODE, 1, 4 ) = substr(b.INDUSTRIESCODE, 1, 4 ) 
            AND b.LEVELNUM = '2' 
            AND a.CUR_SIGN = 1
        '''
    with OracleSql() as oracle:
        data = oracle.query(sql)
    return data


if __name__ == "__main__":
    constituent_df = getConstituent()
    industry_df = getIndustry()
    df = pd.merge(constituent_df, industry_df, on="S_INFO_WINDCODE", how="left")
    df.to_csv("debug.csv", encoding="gbk")
    count_df = df.groupby(by="INDUSTRIESNAME").count()
    print(count_df)
    count_df.to_csv("industry_distribution.csv", encoding="gbk")
