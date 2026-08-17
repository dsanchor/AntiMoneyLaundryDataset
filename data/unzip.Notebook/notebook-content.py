# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9795a299-89c3-47fd-b94a-a8707bd0b534",
# META       "default_lakehouse_name": "raw_data",
# META       "default_lakehouse_workspace_id": "cc9a739c-0800-4ee7-b285-f487739752e5",
# META       "known_lakehouses": [
# META         {
# META           "id": "9795a299-89c3-47fd-b94a-a8707bd0b534"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Creditos Kaggle 
# ## AML
# https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml
# 
# 1a. HI-Small_Trans.csv Transactions
# 1b. HI-Small_Patterns.txt Laundering Pattern Transactions
# 
# 2a. HI-Medium_Trans.csv Transactions
# 2b. HI-Medium_Patterns.txt Laundering Pattern Transactions
# 
# 3a. HI-Large_Trans.csv Transactions
# 3b. HI-Large_Patterns.txt Laundering Pattern Transactions
# 
# 4a. LI-Small_Trans.csv Transactions
# 4b. LI-Small_Patterns.txt Laundering Pattern Transactions
# 
# 5a. LI-Medium_Trans.csv Transactions
# 5b. LI-Medium_Patterns.txt Laundering Pattern Transactions
# 
# 6a. LI-Large_Trans.csv Transactions
# 6b. LI-Large_Patterns.txt Laundering Pattern Transactions
# 
# 
# ## Synthetic  Transactions 
# credit  https://www.kaggle.com/datasets/radistaleks/synthetic-bank-transactions
# 
# Content
# There you have 4 datasets.
# Clients - basic information about bank users.
# Categories - standart transaction categories which are being by many banks worldwide.
# Transactions - the core of our dataset, basic information about transactions like who is the second account of transaction, category, amount, etc.
# Subscriptions - information about subscriptions, in other words, transactions which are made automatically.
# 
# 
# 


# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

import os
import zipfile
import shutil

# IMPORTANTE: en notebooks de Fabric, la ruta local persistente para Files es /lakehouse/default/Files/
# zip_path = "/lakehouse/default/Files/SAN_DC_1_6-29-2026.zip"
zip_path = "/lakehouse/default/Files/TRX_ZIP/archive.zip"
#extract_root = "/lakehouse/default/Files/SAN_DC_1_6-29-2026_unzipped"

extract_root = "/lakehouse/default/Files/trx_unzipped"

# Crear carpeta raíz de salida si no existe
os.makedirs(extract_root, exist_ok=True)

# Descomprimir preservando estructura interna del ZIP
with zipfile.ZipFile(zip_path, "r") as zf:
    for member in zf.infolist():
        # Saltar directorios
        if member.is_dir():
            continue

        rel_path = member.filename              # Ruta relativa dentro del ZIP
        target_path = os.path.join(extract_root, rel_path)  # Ruta completa destino

        # Crear carpeta destino si no existe
        target_dir = os.path.dirname(target_path)
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)

        # Copiar contenido del archivo desde el ZIP al destino
        with zf.open(member, "r") as src, open(target_path, "wb") as dst:
            shutil.copyfileobj(src, dst)

print("Descompresión finalizada en:", extract_root)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
