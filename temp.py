from makeCSI500Portfolio import *

if __name__ == "__main__":
    df = makePortfolio(2000000, "20191216", hundred=False)
    ticker_list = df["证券代码"].tolist()
    df2 = pd.read_excel("his_spot_1212.xlsx", encoding="gbk", skiprows=range(0, 4), index_col=None)
    df2.drop([df2.shape[0] - 1], axis=0, inplace=True)
    df2 = df2[["发生日期", "证券代码", "证券名称", "股份余额"]]
    df2["证券代码"] = df2["证券代码"].astype(int)
    df2["证券代码"] = df2["证券代码"].astype(str)
    df2["证券代码"] = df2["证券代码"].apply(lambda s: s.zfill(6))
    df2["证券代码"] = df2["证券代码"].apply(lambda s: s+ ".SH" if s.startswith('6') else s  + ".SZ")
    # df3 = pd.DataFrame()
    # for key, record in df2.iterrows():
    #     if record["证券代码"] not in ticker_list:
    #         df3 = df3.append(record)
    #
    # df3 = df3[(df3["股份余额"] != 0) & (df3["证券代码"] != "511990.SZ")]
    #
    # print(df3)
    # df3.to_csv("不在500成分股中的股票.csv", encoding="gbk")

    # makeup = set(df2["证券代码"].tolist()) - set(ticker_list)
    makeup = set(ticker_list) - set(df2["证券代码"].tolist())
    df4 = pd.DataFrame(list(makeup), columns=["证券代码"])
    print(df4.to_csv("需要买进的成分股.csv", encoding="gbk", index=None))