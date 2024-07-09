
print("""
.--------------------------------------------------------------------------.
| _   _ ____  ____    _  _____ _____   ____  ____      _    ____ ___ _     |
|| | | |  _ \|  _ \  / \|_   _| ____| | __ )|  _ \    / \  / ___|_ _| |    |
|| | | | |_) | | | |/ _ \ | | |  _|   |  _ \| |_) |  / _ \ \___ \| || |    |
|| |_| |  __/| |_| / ___ \| | | |___  | |_) |  _ <  / ___ \ ___) | || |___ |
| \___/|_|   |____/_/   \_\_| |_____| |____/|_| \_\/_/   \_\____/___|_____||
'--------------------------------------------------------------------------'
""")
#______________
#--- LIBRERIA
#______________
import pandas as pd

#Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico de nombre
#de archivo en un directorio o en una jerarquía de directorios.
import glob

import numpy as np
import os
#import dask.dataframe as dd
from datetime import datetime, timedelta
print("cargo libreria")

#_______________________
#--- EXTRACCION DATOS
#_______________________

ruta_bra= r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Campañas\Data\Sales Brasil.csv'
df_bra= pd.read_csv(ruta_bra, header=0,dtype=str)
print("cargo archivo")


#________________
#--- TIPO DATO
#________________
df_bra['Tipo de dato'] =''

cliente_men=[ 'havan lojas de departamentos',
       'leroy merlin cia bras de', 'martins com e serv dist sa']

def assign_tipo_dato(tipo_dato):
    if tipo_dato.lower() in cliente_men:
        return "men"
    else:
        return "sem"

# Apply the lambda function to the 'Tipo de dato' column
df_bra['Tipo de dato'] = df_bra['Tipo de dato'].apply(assign_tipo_dato)
df_bra['Country'] = 'Brasil'

#--- Seleccion columnas de interes
col=[ 'Customer Group', 'SBU', 'Brand','SKU', ' Sell Out R$', 'Tipo de dato','Country','Fecha']
df_bra=df_bra[col]
print("tipo de dato sem - men")
col_chi=(['num_fila',
       'Date',
       '(L) Retailer',
       '(L) Local',
       '(L) Cadena',
       'canal_venta',
       '(I) SBU', 
       '(I) MARCA',
       '(E) Marca',
       '(I) NPI',
       '(I) GPP Division',
       '(I) GPP Division Cod.',
       '(I) GPP Category',
       '(I) GPP Category Cod.',
       '(I) GPP Portfolio',
       '(I) GPP Portfolio Cod.',
       '(I) Producto Interno', 
       '(I) Código Producto Interno',
       '(I) OGSM Strategy',
       '(I) CORD / CORDLESS / COMB / NEUM',
       'Venta neta',
       'Venta bruta',
       'Venta costo',
       'Unidades vendidas',
       'Volumen vendido (Capacidad 1)',
       'Precio Publico Estimado',
       'Tipo de dato', 
       'Country',
       'concat_update',
       'concat_update_meli_amz',
       'Fecha'])


#________________
#--- COL VACIAS 
#________________

#Funcion que inserta columna vacia 
def crear_columnas_vacias(df, columnas, posiciones):
    for col, pos in zip(columnas, posiciones):
        df.insert(loc=pos, column=col, value='')

#------------
# Brasil       
columnas_vacias = ["vacia0","vacia1",
                   "vacia3","vacia4","vacia5",
                   "vacia8","vacia9","vacia10","vacia11","vacia12","vacia13","vacia14","vacia15","vacia16",
                   "vacia18","vacia19",
                   "vacia21","vacia22","vacia23","vacia24","vacia25",
                   "vacia28","vacia29"]
posiciones = [0,1,3,4,5,8,9,10,11,12,13,14,15,16,18,19,21,22,23,24,25,28,29]
crear_columnas_vacias(df_bra, columnas_vacias, posiciones)

print("columnas vacias")

# --------- RENAME COLUMNAS -------------------------------
col_bra=df_bra.columns.to_list()

dict_renombres_per = {nombre_per: nombre_chi for nombre_per, nombre_chi in zip(col_bra, col_chi)}
df_bra.rename(columns=dict_renombres_per, inplace=True)  # renombrar las columnas utilizando el diccionario


df_bra = df_bra[df_bra['Fecha'].notna()]


#______________________________
#---- TRATAMIENTO COLUMNAS ----
#______________________________


# Eliminar filas con valores NaN en la columna 'Date'
df_bra = df_bra.dropna(subset=['Fecha']) 
# Convertir a int y luego a str

df_bra['(L) Retailer'] =df_bra['(L) Retailer'].str.lower()
df_bra['(I) SBU'] =df_bra['(I) SBU'].str.lower()



df_bra['(I) MARCA'] = df_bra['(I) MARCA'].astype(str).str.lower().fillna('vacio').replace('nan','vacio')

df_bra['(I) Código Producto Interno'] = df_bra['(I) Código Producto Interno'].astype(str).str.lower()
df_bra['Tipo de dato'] = df_bra['Tipo de dato'].astype(str).str.lower()
print("tratamiento columna")

#-- Formato numerico
#df_bra['Venta neta']=df_bra['Venta neta'].str.replace(',', '').astype(float)

#--- TRATAMIENTO NULOS
def vacios_str(columna):
  return columna.fillna('0').replace('', '0')

columnas_object = list(df_bra.select_dtypes(include=['object']).columns)

for columna in columnas_object:
  df_bra[columna] = vacios_str(df_bra[columna])

def vacios_float(columna):
  return columna.fillna(0)
columnas_float = list(df_bra.select_dtypes(include=['float']).columns)
for columna in columnas_float:
  df_bra[columna] = vacios_float(df_bra[columna])

print("nulos")

#_____________
#---MARCAS  
#_____________

lst_marca= [  
 'facom', 'iar expert', 'powers', 'troy-bilt', 'yard machine', 'no usar' ,
 'stanley', 'dewalt', 'b+d', 'irwin', 'proto','bostitch', 'fat max', 'porter cable', 
'lenox', 'craftsman', 'no_usar',  'gridest' 
]

#--- ASIGNACION  

df_bra['(I) MARCA'] = df_bra['(I) MARCA'].replace('vacio', 'other')


correspondencias = {
    'black+decker': ['b/d','black & de', 'black and decker', 'black&decker', 'black & decker', 'black+decker', 'black+deck', 'black + decker', 'b&d', 'b+d', 'black decker', 'black-d', 'black&deck'],
    'dewalt': ['dewalt', 'de walt'],
    'fatmax': ['stanley fatmax', 'fat max', 'fatmax'],
    'bostitch': ['bosch', 'bostitch', 'bostitch office'],
    'no usar': ['einhell','sierra','geo','samoa','smart','no usar']
}

df_bra['(I) MARCA'] = df_bra['(I) MARCA'].apply(lambda x: next((clave for clave, valor in correspondencias.items() if x in valor), x))
df_bra['(I) MARCA'] =df_bra['(I) MARCA'].apply(lambda x:'other' if x not in lst_marca else x)
df_bra['(I) MARCA'] =df_bra['(I) MARCA'].str.upper()


#___SBU
df_bra['(I) SBU'] = df_bra['(I) SBU'].fillna('oth').replace(['',' ','no definido','vacio','nan','finance adjust',], 'oth')


print("marca - sbu")
#______________________
#--- ECOMMERCE LOCAL  -
#______________________
 
lst_canal = 'internet|online|distancia|digital|virtual|ecommerce|e-com'
lst_retailer = 'mercado libre|e-comm|ecommerce|mercadolibre|amazon'



mask_retailer = df_bra['(L) Retailer'].str.contains(lst_retailer)
mask_canal = df_bra['(L) Local'].str.contains(lst_canal)
mask_ecom= (mask_retailer|mask_canal)

df_bra.loc[mask_ecom, 'canal_venta'] = 'ecommerce'



df_bra['canal_venta'] = df_bra['canal_venta'].replace(['vacio','moderno','tradicional','tienda','','0'], 'local')
df_bra['canal_venta'] = df_bra['canal_venta'].replace(['ecommerce','e-commerce'], 'ecommerce')

print("canal venta")
#_______________
#--- HOLDER ----
#_______________
df_bra['Holder']=''
df_bra['Holder']=''
notacion_holder = {
    'Mercado Libre': ['mercadolibre', 'mercado libre multivende', 'mercado libre spiral', 'mercado libre spiral ar'],
    'Sodimac': ['sodimac', 'sodimac mexico', 'sodimac argentina', 'sodimac uruguay', 'sodimac peru'],
    'Amazon': ['amazon mx'],
    'Walmart': ['walmart', 'walmart mexico', 'walmart argentina'],
    'Coppel': ['coppel b'],
    'The Home Depot': ['the home depot e-comm', 'the home depot'],
    'Easy': ['easy argentina']
}
notacion_pais = {
    'Mexico': 'MX',
    'Chile': 'CH',
    'Argentina': 'AR',
    'Uruguay': 'URU',
    'Peru': 'PE',
    'Brasil':'BR',
    'Brazil':'BR'
}

lst = ['mercado libre', 'sodimac', 'walmart', 'the home depot', 'easy', 'amazon']
def holder(row):
    pais = notacion_pais.get(row['Country'], row['Country'])
    retailer = notacion_holder.get(row['(L) Retailer'], row['(L) Retailer']).lower()
    local = row['(L) Local'].lower()

    if pais == 'CH' and retailer == 'mercado libre' and local == 'fcom fcom':
        retailer = 'falabella'
    elif pais == 'CH' and retailer == 'mercado libre' and local == 'paris':
        retailer = 'paris'

    if retailer in lst:
        return retailer +' '+ pais
    else:
        return retailer
df_bra['Holder'] = df_bra.apply(holder, axis=1)
print("holder")

#___________________________
#VENTA USD 
#___________________________
ruta_fx_rate = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Shares Information for Projects\FX_Rate.xlsx'

df_bra['fecha_my'] = pd.to_datetime(df_bra['Fecha']).dt.strftime('%m%Y')

df_fx_rate = pd.read_excel(ruta_fx_rate)
df_fx_rate.drop_duplicates(subset=['Fecha', 'Country'], inplace=True) # Agregar inplace=True para modificar el dataframe original
df_fx_rate['Fecha'] = pd.to_datetime(df_fx_rate['Fecha'])
df_fx_rate['fecha_my'] = df_fx_rate['Fecha'].dt.strftime('%m%Y')
df_fx_rate = df_fx_rate[df_fx_rate['Country'].isin(df_bra['Country']) 
                          & df_fx_rate['fecha_my'].isin(df_bra['fecha_my'])]

df_bra = pd.merge(df_bra, df_fx_rate[['fecha_my', 'Country', 'Adjusted Rate']], on=["fecha_my", "Country"], how='left')
try:
    df_bra['Venta_neta_usd'] = df_bra['Venta neta'] / df_bra['Adjusted Rate']
except:
    df_bra['Venta_neta_usd'] = df_bra['Venta neta']

df_bra['Venta_neta_usd']=df_bra['Venta_neta_usd'].fillna(0)

df_bra.drop(['fecha_my', 'Adjusted Rate'],axis=1,inplace=True)

print("venta usd")
#______________________________
#--- GUARDAR
#______________________________


#--- DRIVE

#Guardar el df_historico como archivo CSV en la ruta deseada
ruta_drive_archivo_bra_csv = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Campañas\Dataflow\Sales_Bra.csv'
df_bra.to_csv(ruta_drive_archivo_bra_csv, index=False)

print("guarda archivo local")
#--- LOCAL
#Guardar el df_historico como archivo CSV en la ruta deseada
ruta_local_archivo_bra_csv = r'C:\Users\SSN0609\Documents\Dashboards LAG PC LOCAL\Data Flow\Campañas\Dataflow\Sales_Bra.csv'
df_bra.to_csv(ruta_local_archivo_bra_csv, index=False)
print("guardo en drive")