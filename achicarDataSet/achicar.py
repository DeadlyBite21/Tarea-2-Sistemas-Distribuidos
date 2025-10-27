import pandas as pd

# Lee el archivo original
df = pd.read_csv('/Users/benjaminzunigapueller/Documents/GitHub/Tarea-2-Sistemas-Distribuidos/achicarDataSet/train.csv')

# Selecciona las primeras 10,000 líneas
df_recortado = df.head(10000)

# Guarda el nuevo archivo
df_recortado.to_csv('Train2.csv', index=False)