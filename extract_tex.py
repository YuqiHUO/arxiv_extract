import os
import shutil
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def extract_arxiv(input_dir, output_dir):
    """
    从arxiv数据中提取latex源码(.tex文件)，并保存到parquet文件中
    """
    # 用于存储每个.tex文件路径和内容的列表
    src_text_list = []

    # 统计总文件数和成功处理的文件数
    total_files = 0
    success_files = 0

    # 遍历input_dir下的所有文件夹
    for arxiv_id in os.listdir(input_dir):
        try:
            # 文件夹路径
            folder_path = os.path.join(input_dir, arxiv_id)

            # source文件路径
            src_file_path = os.path.join(folder_path, 'source', arxiv_id)

            # 解压缩文件夹路径
            extract_folder_path = os.path.join(os.getcwd(), 'temp_' + arxiv_id)

            # 如果此文件夹不存在，创建它
            os.makedirs(extract_folder_path, exist_ok=True)
            
            # 拷贝并重命名文件到解压缩文件夹
            src_file_path_gz = os.path.join(extract_folder_path, arxiv_id + '.tar.gz')
            shutil.copy(src_file_path, src_file_path_gz)

            # 解压缩文件
            shutil.unpack_archive(src_file_path_gz, extract_folder_path)

            # 遍历解压后的文件夹，查找.tex文件
            for root, dirs, files in os.walk(extract_folder_path):
                for file in files:
                    # 如果是.tex文件
                    if file.endswith('.tex'):
                        # 文件路径
                        file_path = os.path.join(root, file)
                        relative_path = arxiv_id + file_path.replace(extract_folder_path, '')
                        # 读取.tex文件内容
                        with open(file_path, 'r', errors='ignore') as f:
                            text = f.read()
                        # 将文件路径和内容添加到列表中
                        src_text_list.append((relative_path, text))

            success_files += 1
        except Exception as e:
            print(f"Failed to process file {arxiv_id} due to error: {e}")
        finally:
            # 无论是否成功，都删除临时文件夹
            shutil.rmtree(extract_folder_path)
        
        total_files += 1

    # 计算成功率
    success_rate = success_files / total_files
    print(f"Success rate: {success_rate * 100:.2f}%")

    # 创建Pandas DataFrame
    df = pd.DataFrame(src_text_list, columns=['src', 'text'])

    # 使用pyarrow将DataFrame转换为parquet格式
    table = pa.Table.from_pandas(df)
    pq.write_table(table, os.path.join(output_dir, 'arxiv_data.parquet'))


if __name__ == "__main__":
    input_dir = "arxiv-subset-100"
    output_dir = "extract"
    extract_arxiv(input_dir, output_dir)
