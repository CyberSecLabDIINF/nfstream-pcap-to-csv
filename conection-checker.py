import pandas as pd

suma_total = 0
chunksize = 1000000 # 1 millón de filas

for chunk in pd.read_csv('/archivo.csv', usecols=['src2dst_packets', 'dst2src_packets'], chunksize=chunksize):
    suma_total += chunk['src2dst_packets'].sum() + chunk['dst2src_packets'].sum()

print(f"Suma completa: {suma_total}")
