import pandas as pd
import io
from PIL import Image

# Load parquet file
df = pd.read_parquet('data/extract/arxiv_data.parquet')

# Print the DataFrame's outline
print(df.head())

# Assume the image is stored in the 0th row and 'content' column
image_data = df.loc[0, 'content']

# Convert binary data to PIL image
image = Image.open(io.BytesIO(image_data))

# Save the image
image.save('output_image.jpg')
