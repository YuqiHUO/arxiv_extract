import os
import shutil
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
import zipfile
import glob
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from PIL import Image
import io
from tqdm import tqdm

def extract_arxiv(directory):
    data = []
    dir_count = sum([len(d) for r, d, f in os.walk(directory)])
    pbar = tqdm(total=dir_count, desc="Processing directories")

    for root, dirs, files in os.walk(directory):
        arxiv_id = os.path.basename(root)
        for file in files:
            filepath = os.path.join(root, file)
            # Check if the file is a pdf
            if file.endswith('.pdf'):
                # Confirm only one .pdf file in the pdf directory, else return None
                if len(files) != 1:
                    return None
                
                # Read pdf and convert each page to jpg
                pdf = PdfReader(filepath)
                images = convert_from_path(filepath)
                for i in range(len(images)):
                    image = images[i]
                    image_path = f"{filepath[:-4]}_{i}.jpg"
                    image.save(image_path, "JPEG")

                with open(image_path, 'rb') as img_file:
                    content = img_file.read()
                data.append((arxiv_id, '图片', f'{arxiv_id}_{i}', content))

            # Check if the file is a source file
            elif file == arxiv_id:
                # Unzip source file
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    zip_ref.extractall(root)

                # Find all .tex, .eps, .png, .pdf, .jpg, .jpeg files
                extensions = ('**/*.tex', '**/*.eps', '**/*.png', '**/*.pdf', '**/*.jpg', '**/*.jpeg')
                for extension in extensions:
                    for tex_file in glob.glob(os.path.join(root, extension), recursive=True):
                        # Read .tex file content as text
                        if extension == '**/*.tex':
                            with open(tex_file, 'r') as file:
                                content = file.read()
                            data.append((arxiv_id, '文本', os.path.basename(tex_file)[:-4], content))
                        else:
                            # Read other file content as binary
                            with open(tex_file, 'rb') as file:
                                content = file.read()
                            data.append((arxiv_id, '图片', os.path.basename(tex_file)[:-4], content))

        pbar.update(1)
    
    pbar.close()
    return data

def save_to_parquet(data, filename):
    # Convert data to pandas DataFrame
    df = pd.DataFrame(data, columns=['arxiv_id', 'type', 'name', 'content'])
    
    # Convert to pyarrow Table
    table = pa.Table.from_pandas(df)
    
    # Write to parquet
    pq.write_table(table, filename)

directory = "data/raw/arxiv-subset-100"
data = extract_arxiv(directory)
save_to_parquet(data, "data/extract/arxiv_data.parquet")
