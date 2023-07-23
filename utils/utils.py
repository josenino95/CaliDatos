import math
import pandas as pd
import re
from unicodedata import normalize
import numpy as np
from tqdm import trange

def dif_tiempo_a_texto(time):
    tiempo = str(math.floor(time / 86400)) + "d, " +  \
    str(math.floor(time % 86400 / 3600)) + "hr, " +  \
    str(math.floor((time % 86400) % 3600 / 60)) + "min, " +  \
    str(math.floor((((time % 86400) % 3600) % 60)*1000)/1000.0) + "sec "
    return tiempo

def lectura_datos(fecha_corte, archivo_datos, hoja_datos, rango_columnas=None, inicial_fila=None):
    # Leer datos
    df = pd.read_excel(archivo_datos, sheet_name=hoja_datos, usecols=rango_columnas, skiprows=inicial_fila-1)
    # Leer nombres de columnas y generar mensaje si hay o no nombres repetidos
    columnas = pd.read_excel(archivo_datos, sheet_name=hoja_datos, usecols=rango_columnas, skiprows=inicial_fila-1, header=None, nrows=1)
    columnas_repetidas = list(columnas.iloc[0,:][columnas.iloc[0,:].duplicated()].values)

    nombre_hoja_resultados = '1.SugerenciaNombresVariables'
    if len(columnas_repetidas) == 0:
        mensaje = 'Se leyó el conjunto de datos. \n No hay columnas con nombres repetidos. \n Se agregó al archivo de resultados la hoja {} con las sugerencias de nombres de las variables'.format(nombre_hoja_resultados)
        print(mensaje)
    else:
        mensaje = 'Se leyó el conjunto de datos. \n Los siguientes nombres de columnas están repetidos, por favor cambie los nombres antes de continuar \n {}. \n Se agregó al archivo de resultados la hoja {} con las sugerencias de nombres de las variables'.format(columnas_repetidas, nombre_hoja_resultados)
        print(mensaje)

    # Creación de archivo resumen con los resultados del análisis de calidad
    df_resumen_general = pd.DataFrame({"Nombre indicador": ["Fase 1. Lectura de datos. Número de registros en los datos", "Fase 1. Lectura de datos. Número de columnas/variables en los datos"], 
                        "Cantidad en el corte " + fecha_corte: [df.shape[0], df.shape[1]],
                        "Porcentaje en el corte " + fecha_corte: ["", ""]
                        }
                        )

    # Generar archivo con nombres sugeridos, si es necesario hacer algun cambio
    lista_columnas = list(columnas.iloc[0,:].values)
    columnas_sugeridas = list(map(lambda string: \
                    normalize("NFKD", string).encode("ascii","ignore").decode("ascii") \
                        .replace('\n', ' ').replace(' ', '_').replace('(', '').replace(')', '').upper() \
                    , lista_columnas))
    columnas_sugeridas = [v + '_' + str(columnas_sugeridas[:i].count(v) + 1) if columnas_sugeridas.count(v) > 1 else v for i, v in enumerate(columnas_sugeridas)]
    
    sugerencia_nombres = pd.DataFrame({'Campo': lista_columnas, 'Nombre de columna sugerido': columnas_sugeridas})
    sugerencia_nombres["Nombre de columna sugerido"] = np.where(sugerencia_nombres["Nombre de columna sugerido"]==sugerencia_nombres["Campo"], \
    "", sugerencia_nombres["Nombre de columna sugerido"])

    with pd.ExcelWriter("Resultados_Calidad_" + fecha_corte + ".xlsx") as writer:
                df_resumen_general.to_excel(writer, sheet_name="Resumen general", index=False)
                sugerencia_nombres.to_excel(writer, sheet_name=nombre_hoja_resultados, index=False)


    return df, mensaje

def identificar_tipo_dominio(dominio_campo):
    if dominio_campo == 'nan' or dominio_campo == '' :
        tipo_dominio_i = '7. No disponible'
    elif dominio_campo.startswith('[') and dominio_campo.endswith(']'):
        tipo_dominio_i = '1. Intervalo Cerrado-Cerrado'
    elif dominio_campo.startswith('[') and dominio_campo.endswith(')'):
        tipo_dominio_i = '2. Intervalo Cerrado-Abierto'
    elif dominio_campo.startswith('(') and dominio_campo.endswith(']'):
        tipo_dominio_i = '3. Intervalo Abierto-Cerrado'
    elif dominio_campo.startswith('(') and dominio_campo.endswith(')'):
        tipo_dominio_i = '4. Intervalo Abierto-Abierto'
    elif dominio_campo.upper().startswith('VER TABLA'):
        tipo_dominio_i = '5. Tabla anexa'
    else:
        tipo_dominio_i = '6. Lista (Separado por comas)'
    return tipo_dominio_i

def lectura_diccionario(archivo_datos, hoja_datos, rango_columnas=None, inicial_fila=None):
    df = pd.read_excel(archivo_datos, sheet_name=hoja_datos, usecols=rango_columnas, skiprows=inicial_fila-1)
    # Inicializar elementos que se van a guardar
    columnas_dicc = []
    llave_primaria = []
    campos_obligatorios = []
    tipo_dominio = {}
    dominio = {}
    tipo_dato = {}
    longitud = {}
    nom_hoja_dominio_previa = ''
    for i in trange(df.shape[0]):
        # Recuperar el nombre del campo y que se van a incluir en los diccionarios y listas
        nombre_campo = df.iloc[i,:]['campo']
        # Agregar campo en el listado de columnas
        columnas_dicc.append(nombre_campo)
        # Si corresponde a una llave primaria, agregar a lista correspondiente    
        if str(df.iloc[i,:]['llave_primaria']).upper().strip() == "SI":
            llave_primaria.append(nombre_campo)
        # Si es un campo obligatorio, agregar a lista correspondiente    
        if str(df.iloc[i,:]['obligatorio']).upper().strip() == "SI":
            campos_obligatorios.append(nombre_campo)
        # Identificar el tipo de dominio correspondiente al campo
        ## Leer la variable de dominio
        dominio_campo = str(df.iloc[i,:]['dominio']).strip()
        # Identificar tipo de dominio con función previamente definida
        tipo_dominio_i = identificar_tipo_dominio(dominio_campo)
        # Añadir tipo de dominio a diccionario de dominios
        tipo_dominio[nombre_campo] = tipo_dominio_i
        # Guardar tipo de dato almacenado
        tipo_dato[nombre_campo] = df.iloc[i,:]['tipo']
        # Dependiendo del tipo de dominio, guardar en un diccionario su dominio
        ## Guardar diccionario con minimo y maximo para los de tipo intervalo
        if tipo_dominio_i in ['1. Intervalo Cerrado-Cerrado', '2. Intervalo Cerrado-Abierto', '3. Intervalo Abierto-Cerrado', '4. Intervalo Abierto-Abierto']:
            dominio[nombre_campo] = {
            'minimo': float(re.search('\[(.*?),', dominio_campo).group(1)),
            'maximo': float(re.search(',(.*?)\]', dominio_campo).group(1))}
        ## Guardar lista con posibles valores que vienen del diccionario
        elif tipo_dominio_i == '6. Lista (Separado por comas)':
            listado_domino = [x.strip() for x in dominio_campo.split(',')]
            if tipo_dato[nombre_campo] == 'Numérico':
                 dominio[nombre_campo] = [float(x) for x in listado_domino]
            elif tipo_dato[nombre_campo] == 'Entero':
                 dominio[nombre_campo] = [float(x) for x in listado_domino]
            else:
                 dominio[nombre_campo] = listado_domino                     
                 
        ## Guardar lista con posibles valores que vienen de tabla anexa
        elif tipo_dominio_i == '5. Tabla anexa':
            nom_hoja_dominio = re.search('(?<=VER TABLA ).*', dominio_campo, re.IGNORECASE).group(0)
            # Leer archivo solo si no se ha leido antes
            if nom_hoja_dominio_previa != nom_hoja_dominio:
                tabla_anexa = pd.read_excel(archivo_datos, sheet_name=nom_hoja_dominio)
                nom_hoja_dominio_previa = nom_hoja_dominio
            dominio[nombre_campo] = list(tabla_anexa[nombre_campo].squeeze().unique())
        else:
            dominio[nombre_campo] = ''    
        # Guardar longitud del campo
        longitud[nombre_campo] = df.iloc[i,:]['longitud']
    mensaje = 'Se completó el proceso de forma exitosa'
    return df, columnas_dicc, llave_primaria, campos_obligatorios, tipo_dominio, dominio, tipo_dato, longitud, mensaje
