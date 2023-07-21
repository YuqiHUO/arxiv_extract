import pandas as pd

def test_parquet(file_path):
    # 使用pandas读取parquet文件
    df = pd.read_parquet(file_path)

    # 打印文件内容
    print(df)

    # 打印文件统计信息
    print(df.describe())

if __name__ == "__main__":
    test_parquet("extract/arxiv_data.parquet")
