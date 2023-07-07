import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm, trange
import numpy as np
import re
from pandas import Timestamp
import math
from unicodedata import normalize
import sys

def dif_tiempo_a_texto(time):
    tiempo = str(math.floor(time / 86400)) + "d, " +  \
    str(math.floor(time % 86400 / 3600)) + "hr, " +  \
    str(math.floor((time % 86400) % 3600 / 60)) + "min, " +  \
    str(math.floor((((time % 86400) % 3600) % 60)*1000)/1000.0) + "sec "
    return tiempo

def creacion_log(ruta_archivo_log):
    df_log = pd.DataFrame({"ID": [1], 
                        "FASE": 'Fase 0',
                        "ACCIÓN": 'Inicio log',
                        "DESCRIPCIÓN": 'Se crea archivo de log calidad de datos',
                        "HORA_INICIO_EJECUCIÓN":[Timestamp.now()],
                        "HORA_FIN_EJECUCIÓN":[Timestamp.now()],
                        "DURACIÓN":[0],
                        "MENSAJE": ''
                        }
                        )
    df_log.to_excel(ruta_archivo_log, index=False)
    print('Se creo archivo log {}'.format(ruta_archivo_log))

    return df_log

def inicio_log(df_log, fase, accion, descripcion):
    try:
        hora_inicio=Timestamp.now()
        fase = fase
        accion = accion
        descripcion = descripcion

        registro={"ID": [df_log.iloc[-1]['ID']+1],  
                "FASE":fase,
                "ACCIÓN":accion,
                "DESCRIPCIÓN":descripcion,
                "HORA_INICIO_EJECUCIÓN": [hora_inicio],
                "HORA_FIN_EJECUCIÓN": [hora_inicio],
                "DURACIÓN": '',
                "MENSAJE": ''
                }
        df_log= pd.concat([df_log, pd.DataFrame(registro)], axis=0, ignore_index=True)
        return df_log
    except ValueError:
        print(ValueError)

def fin_log(df_log, ruta_archivo_log, mensaje):
    try:
        hora_fin = Timestamp.now()
        hora_inicio = df_log.loc[df_log.index[-1], 'HORA_INICIO_EJECUCIÓN']
        tiempo = dif_tiempo_a_texto((hora_fin-hora_inicio).total_seconds())
        df_log.loc[df_log.index[-1], 'HORA_FIN_EJECUCIÓN'] = [hora_fin]
        df_log.loc[df_log.index[-1], 'DURACIÓN'] = tiempo
        df_log.loc[df_log.index[-1], 'MENSAJE'] = mensaje
        df_log.to_excel(ruta_archivo_log, index=False)
        print('Se actualizó el archivo log {}'.format(ruta_archivo_log))
        return df_log
    except ValueError:
        print(ValueError)


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

def validar_variables(fecha_corte, df_datos, cols_dicc, parar_si_errores=True):
    # Leer archivo resumen
    with pd.ExcelFile("Resultados_Calidad_" + fecha_corte + ".xlsx") as xls:
        df_resumen_general = pd.read_excel(xls, sheet_name="Resumen general")                            
    
    cols_datos = list(df_datos.columns)
    ncols_datos = len(cols_datos)
    ncols_dicc = len(cols_dicc)
    dicc_no_en_datos = [x for x in cols_dicc if x not in cols_datos]
    datos_no_en_dicc = [x for x in cols_datos if x not in cols_dicc]
    ndicc_no_en_datos = len(dicc_no_en_datos)
    ndatos_no_en_dicc = len(datos_no_en_dicc)
    todas = cols_datos + dicc_no_en_datos

    if ndicc_no_en_datos==0 and ndatos_no_en_dicc==0:
        mensaje = 'Total de campos en datos {}. \n Total de campos en diccionario {}. \
             \n Todos los campos coinciden'.format(ncols_datos, ncols_dicc)
    elif ndicc_no_en_datos>0 and ndatos_no_en_dicc==0:
        mensaje = 'Total de campos en datos {}. \n Total de campos en diccionario {}. \
             \n Los siguientes campos están en el diccionario pero no en los datos \n {}'.format(ncols_datos, ncols_dicc, dicc_no_en_datos)
    elif ndicc_no_en_datos==0 and ndatos_no_en_dicc>0:
        mensaje = 'Total de campos en datos {}. \n Total de campos en diccionario {}. \
             \n Los siguientes campos están en los datos pero no en el diccionario \n {}'.format(ncols_datos, ncols_dicc, datos_no_en_dicc)
    elif ndicc_no_en_datos>0 and ndatos_no_en_dicc>0:
        mensaje = 'Total de campos en datos {}. \n Total de campos en diccionario {}. \
             \n Los siguientes campos están en los datos pero no en el diccionario \n {}. \n Los siguientes campos están en el diccionario pero no en los datos {}'.format(ncols_datos, ncols_dicc, datos_no_en_dicc, dicc_no_en_datos)
    
    registro_resumen_general = {"Nombre indicador": ["Fase 2. Revisión de estructura. Variables/Columnas que están en los datos pero no en el diccionario", "Fase 2. Revisión de estructura. Variables/Columnas que están en el diccionario pero no en los datos"],  
                "Cantidad en el corte " + fecha_corte: [ndatos_no_en_dicc, ndicc_no_en_datos],
                "Porcentaje en el corte " + fecha_corte: ["{:.2%}".format(ndatos_no_en_dicc/ncols_datos), "{:.2%}".format(ndicc_no_en_datos/ncols_datos)]
                }

    df_resumen_general= pd.concat([df_resumen_general, pd.DataFrame(registro_resumen_general)], axis=0, ignore_index=True)

    df_resumen_campos = pd.DataFrame()
    df_resumen_campos["Campos"] = todas
    df_resumen_campos = df_resumen_campos.set_index("Campos")
    df_resumen_campos["2. Estructura. Correspondencia entre datos y diccionario"] = ""

    for i in todas:
        if i in dicc_no_en_datos:
            df_resumen_campos.loc[i, "2. Estructura. Correspondencia entre datos y diccionario"] = "No. Está en diccionario pero no en datos"
        elif i in datos_no_en_dicc:
            df_resumen_campos.loc[i, "2. Estructura. Correspondencia entre datos y diccionario"] = "No. Está en datos pero no en diccionario"
        else:
            df_resumen_campos.loc[i, "2. Estructura. Correspondencia entre datos y diccionario"] = "Si"

    df_resumen_campos = df_resumen_campos.reset_index()
    
    with pd.ExcelWriter("Resultados_Calidad_"+fecha_corte+".xlsx", \
        mode="a", if_sheet_exists="replace") as writer:
            df_resumen_general.to_excel(writer, sheet_name="Resumen general", index=False)
            df_resumen_campos.to_excel(writer, sheet_name="Resumen campos", index=False)

    # Si se tiene activada la opción de parar_si_errores==True, se detiene la ejecución del código ante
    # la no coincidencia entre los datos y el diccionario
    if (parar_si_errores==True) and (len(dicc_no_en_datos)>0 or len(datos_no_en_dicc)>0):
        print("Se ha detenido la ejecución porque los campos no coinciden entre el diccionario y los datos. \n Por favor realice los ajustes correspondientes y vuelva a ejecutar el código.")
        print(mensaje)
        sys.exit("No hay coincidencia entre los campos del diccionario y los campos en los datos.")

    return mensaje

def asignar_tipo(df_datos, tipo_dato_dicc):
    cols_dicc = list(tipo_dato_dicc.keys())
    cols_datos = list(df_datos.columns)
    cols_coinciden = [x for x in cols_dicc if x in cols_datos]

    # Iniciar loop por cada variable para asignar el tipo correspondiente de acuerdo con el diccionario
    for i in cols_coinciden:
        if tipo_dato_dicc[i] == 'Cadena':
            cast_var_pandas = 'object'
        elif tipo_dato_dicc[i] == 'Entero' or tipo_dato_dicc[i] == 'Entero grande':
            cast_var_pandas = 'Int64'
        elif tipo_dato_dicc[i] == 'Numérico':
            cast_var_pandas = 'float64'
        else:
            cast_var_pandas = 'object'
        
        try:
            df_datos[i] = df_datos[i].astype(cast_var_pandas)
        
        except (ValueError, TypeError) as error:
            print("Se está intentando asignar a la variable {} el tipo de dato {} - {}. Antes de continuar, por favor revise los datos de esta variable y confirme que todos coinciden con el tipo de dato definido en el diccionario".format(i, cast_var_pandas, tipo_dato_dicc[i]))
            print(error)
            sys.exit("Error de asignación de tipo de dato.")
    
    mensaje = 'Se completó el proceso de forma exitosa'
    return df_datos, mensaje

def validar_obligatorios(fecha_corte, df_datos, llaves_primarias, columnas_dicc, campos_obligatorios):
    # Leer archivo resumen
    with pd.ExcelFile("Resultados_Calidad_" + fecha_corte + ".xlsx") as xls:
        df_resumen_general = pd.read_excel(xls, sheet_name="Resumen general")
        df_resumen_campos = pd.read_excel(xls, sheet_name="Resumen campos")

    nobs = len(df_datos) # Número total de registros para posteriormente calcular porcentaje
    cols_datos = list(df_datos.columns)
    cols_coinciden = [x for x in columnas_dicc if x in cols_datos]

    campos_no_llaves = [x for x in cols_coinciden if x not in llaves_primarias] # Campos que no son llaves
    campos_no_llaves_obl = [x for x in campos_obligatorios if x not in llaves_primarias] # Campos obligatorios que no son llaves
    df_obl = df_datos[llaves_primarias + campos_no_llaves_obl] # Subconjunto de los datos solo con las llaves primarias y los campos obligatorios
    df_salida = df_obl.copy().astype(str) # Crear un df que almacenará los registros con error porque tiene vacíos en campos obligatorios
    resumen_total = pd.DataFrame(columns = ['Campos', '3. Completitud. Número de registros vacíos', '3. Completitud. Porcentaje de registros vacíos']) # Iniciar dataframe resumen 
    
    for i in tqdm(campos_no_llaves):
        # Contar cantidad de valores vacíos
        cant_vacios = df_datos[i].isna().sum()
        resumen_total_i = pd.DataFrame({'Campos': i, '3. Completitud. Número de registros vacíos': [cant_vacios], '3. Completitud. Porcentaje de registros vacíos': "{:.2%}".format((cant_vacios/nobs))})
        resumen_total = pd.concat([resumen_total, resumen_total_i])
        # Si el campo es obligatorio, agregar como error en otra hoja
        if i in campos_no_llaves_obl:
            df_salida[i] = np.where(df_obl[i].isna(), 'Error: Campo obligatorio sin registro', None) # Cuando se identifica un nulo, escribir el mensaje de error

    df_salida = df_salida.dropna(how='all', subset=campos_no_llaves_obl) # Eliminar aquellos registros que no tienen error en ningun campo
    df_salida = df_salida.dropna(axis=1, how='all') # Eliminar columnas sin ningún error

    cant_errores = len(df_salida)
    porcentaje_errores = cant_errores/nobs
    registro_resumen_general = {"Nombre indicador": ["Fase 3. Calidad. Completitud. Registros/Filas en los que algún campo obligatorio está vacío"],  
                "Cantidad en el corte " + fecha_corte: [cant_errores],
                "Porcentaje en el corte " + fecha_corte: ["{:.2%}".format(porcentaje_errores)]
                }
    df_resumen_general= pd.concat([df_resumen_general, pd.DataFrame(registro_resumen_general)], axis=0, ignore_index=True)
    df_resumen_campos = df_resumen_campos.merge(resumen_total, on="Campos", how="left") 

    with pd.ExcelWriter("Resultados_Calidad_"+fecha_corte+".xlsx", \
        mode="a", if_sheet_exists="replace") as writer:
            df_resumen_general.to_excel(writer, sheet_name="Resumen general", index=False)
            df_resumen_campos.to_excel(writer, sheet_name="Resumen campos", index=False) 
    
    nombre_hoja_resultados = '3.Completitud'
    if len(df_salida)==0:
        mensaje = 'Todos los campos obligatorios están correctamente diligenciados.'
    else:
        with pd.ExcelWriter("Resultados_Calidad_" + fecha_corte + ".xlsx", \
            mode="a", if_sheet_exists="replace") as writer:
                df_salida.to_excel(writer, sheet_name=nombre_hoja_resultados, index=False)
        mensaje = 'Hay campos obligatorios que tienen registros vacíos. \n Revise la hoja {} para ver cuáles registros tienen este problema'.format(nombre_hoja_resultados)

    return mensaje

def validar_dominio(fecha_corte, df_datos, llaves_primarias, tipo_dominio, dominio):
    # Leer archivo resumen
    with pd.ExcelFile("Resultados_Calidad_" + fecha_corte + ".xlsx") as xls:
        df_resumen_general = pd.read_excel(xls, sheet_name="Resumen general")
        df_resumen_campos = pd.read_excel(xls, sheet_name="Resumen campos")

    cols_dicc = list(tipo_dominio.keys())
    cols_datos = list(df_datos.columns)
    cols_coinciden = [x for x in cols_dicc if x in cols_datos]
    nobs = len(df_datos) # Número total de registros para posteriormente calcular porcentaje

    campos_no_llaves = [x for x in cols_coinciden if x not in llaves_primarias] # Campos que no son llaves

    df_salida = df_datos.copy().astype(str) # Copia en la que se registrará los registros fuera de dominio
    df_salida = df_salida[cols_coinciden] # Mantener solo columnas que coinciden entre datos y diccionario
    resumen_dominio = pd.DataFrame(columns = ['Campos', '3. Número de registros fuera de dominio', '3. Porcentaje de registros fuera de dominio']) # Iniciar dataframe resumen

    for i in tqdm(campos_no_llaves):
        tipo_dominio_i = tipo_dominio[i]
        # Recuperar dominio según el tipo de dominio definido
        if tipo_dominio_i in ['1. Intervalo Cerrado-Cerrado', '2. Intervalo Cerrado-Abierto', '3. Intervalo Abierto-Cerrado', '4. Intervalo Abierto-Abierto']:
            val_minimo = dominio[i]['minimo']
            val_maximo = dominio[i]['maximo']
        elif tipo_dominio_i in ['5. Tabla anexa', '6. Lista (Separado por comas)']:
            lista_dominio = dominio[i]
        # Chequear si registros están en dominio, de acuerdo al tipo de dominio
        df_i = df_datos[i]
        if tipo_dominio_i == '1. Intervalo Cerrado-Cerrado':
            df_salida[i] = np.where(((df_i < val_minimo) | (df_i > val_maximo)) & df_i.notna(), \
                "Error. Dato: " + df_i.astype(str) + ". Fuera del dominio", None) # Cuando se identifica fuera del rango, escribir el mensaje de error
        elif tipo_dominio_i == '2. Intervalo Cerrado-Abierto':
            df_salida[i] = np.where(((df_i < val_minimo) | (df_i >= val_maximo)) & df_i.notna(), \
                "Error. Dato: " + df_i.astype(str) + ". Fuera del dominio", None)
        elif tipo_dominio_i == '3. Intervalo Abierto-Cerrado':
            df_salida[i] = np.where(((df_i <= val_minimo) | (df_i > val_maximo)) & df_i.notna(), \
                "Error. Dato: " + df_i.astype(str) + ". Fuera del dominio", None)
        elif tipo_dominio_i == '4. Intervalo Abierto-Abierto':
            df_salida[i] = np.where(((df_i <= val_minimo) | (df_i >= val_maximo)) & df_i.notna(), \
                "Error. Dato: " + df_i.astype(str) + ". Fuera del dominio", None)
        elif tipo_dominio_i in ['5. Tabla anexa', '6. Lista (Separado por comas)']:
            df_salida[i] = df_i.apply(lambda x: None if pd.isna(x) else (None if x in lista_dominio else "Error. Dato: " + str(x) + ". Fuera del dominio"))
        elif tipo_dominio_i == '7. No disponible':
            df_salida[i] = None

        cant_fuera = df_salida[i].notna().sum() # Contabilizar errores por cada campo
        resumen_i = pd.DataFrame({'Campos': i, '3. Número de registros fuera de dominio': [cant_fuera], '3. Porcentaje de registros fuera de dominio': "{:.2%}".format((cant_fuera/nobs))})
        resumen_dominio = pd.concat([resumen_dominio, resumen_i])

    df_salida = df_salida.dropna(how='all', subset=campos_no_llaves) # Eliminar aquellos registros que no tienen error en ningun campo
    df_salida = df_salida.dropna(axis=1, how='all') # Eliminar columnas sin ningún error
    registro_resumen_general = {"Nombre indicador": ["Fase 3. Calidad. Dominio. Registros/Filas fuera de dominio"],  
                "Cantidad en el corte " + fecha_corte: [len(df_salida)],
                "Porcentaje en el corte " + fecha_corte: ["{:.2%}".format((len(df_salida)/nobs))]
                }
    df_resumen_general= pd.concat([df_resumen_general, pd.DataFrame(registro_resumen_general)], axis=0, ignore_index=True)
    
    df_resumen_campos = df_resumen_campos.merge(resumen_dominio, on="Campos", how="left") 

    nombre_hoja_resultados = '3.Validez_Dominio'

    with pd.ExcelWriter("Resultados_Calidad_" + fecha_corte + ".xlsx", \
        mode="a", if_sheet_exists="replace") as writer:
            df_resumen_general.to_excel(writer, sheet_name="Resumen general", index=False)
            df_resumen_campos.to_excel(writer, sheet_name="Resumen campos", index=False)

    if len(df_salida)==0:
        mensaje = 'Todos los registros están en el dominio definido por el diccionario.'
    else:
        with pd.ExcelWriter("Resultados_Calidad_" + fecha_corte + ".xlsx", \
            mode="a", if_sheet_exists="replace") as writer:
                df_salida.to_excel(writer, sheet_name=nombre_hoja_resultados, index=False)
        mensaje = 'Se encontraron campos con registros fuera del dominio. \n Revise la hoja {} para ver cuáles registros tienen este problema'.format(nombre_hoja_resultados)

    return mensaje


def validar_longitud(fecha_corte, df_datos, llaves_primarias, longitud):
    # Leer archivo resumen
    with pd.ExcelFile("Resultados_Calidad_" + fecha_corte + ".xlsx") as xls:
        df_resumen_general = pd.read_excel(xls, sheet_name="Resumen general")
        df_resumen_campos = pd.read_excel(xls, sheet_name="Resumen campos")

    cols_dicc = list(longitud.keys())
    cols_datos = list(df_datos.columns)
    cols_coinciden = [x for x in cols_dicc if x in cols_datos]
    nobs = len(df_datos) # Número total de registros para posteriormente calcular porcentaje
    campos_no_llaves = [x for x in cols_coinciden if x not in llaves_primarias] # Campos que no son llaves
    
    df_salida = df_datos.copy().astype(str) # Copia en la que se registrará los registros con longitud mayor a lo estipulado
    df_salida = df_salida[cols_coinciden] # Mantener solo columnas que coinciden entre datos y diccionario
    resumen_longitud = pd.DataFrame(columns = ['Campos', '3. Número de registros con longitud mayor a lo estipulado', '3. Porcentaje de registros con longitud mayor a lo estipulado']) # Iniciar dataframe resumen

    for i in tqdm(campos_no_llaves):
        df_i = df_datos[i]
        longitud_dato_i = df_i.astype(str).str.len()
        longitud_dicc_i = longitud[i]
        # Si el campo tiene una longitud mayor a lo definido y no es nulo, arrojar error
        df_salida[i] = np.where((longitud_dato_i > longitud_dicc_i) & df_i.notna(), \
                        "Error. Dato: " + df_i.astype(str) + ". Longitud: " + longitud_dato_i.astype(str) + \
                            ". Longitud estipulada: " + longitud_dicc_i.astype(str), None)

        cant_fuera = df_salida[i].notna().sum() # Contabilizar errores por cada campo
        resumen_i = pd.DataFrame({'Campos': i, \
            '3. Número de registros con longitud mayor a lo estipulado': [cant_fuera], \
            '3. Porcentaje de registros con longitud mayor a lo estipulado': "{:.2%}".format((cant_fuera/nobs))})
        resumen_longitud = pd.concat([resumen_longitud, resumen_i])

    df_salida = df_salida.dropna(how='all', subset=campos_no_llaves) # Eliminar aquellos registros que no tienen error en ningun campo
    df_salida = df_salida.dropna(axis=1, how='all') # Eliminar columnas sin ningún error
    registro_resumen_general = {"Nombre indicador": ["Fase 3. Calidad. Longitud. Registros/Filas con longitud mayor a lo estipulado"],  
                "Cantidad en el corte " + fecha_corte: [len(df_salida)],
                "Porcentaje en el corte " + fecha_corte: ["{:.2%}".format((len(df_salida)/nobs))]
                }
    df_resumen_general= pd.concat([df_resumen_general, pd.DataFrame(registro_resumen_general)], axis=0, ignore_index=True)
    
    df_resumen_campos = df_resumen_campos.merge(resumen_longitud, on="Campos", how="left") 

    nombre_hoja_resultados = '3.Validez_Longitud'

    with pd.ExcelWriter("Resultados_Calidad_" + fecha_corte + ".xlsx", \
        mode="a", if_sheet_exists="replace") as writer:
            df_resumen_general.to_excel(writer, sheet_name="Resumen general", index=False)
            df_resumen_campos.to_excel(writer, sheet_name="Resumen campos", index=False)

    if len(df_salida)==0:
        mensaje = 'Todos los registros tienen la longitud estipulada el diccionario.'
    else:
        with pd.ExcelWriter("Resultados_Calidad_" + fecha_corte + ".xlsx", \
            mode="a", if_sheet_exists="replace") as writer:
                df_salida.to_excel(writer, sheet_name=nombre_hoja_resultados, index=False)
        mensaje = 'Se encontraron campos con longitud mayor a lo estipulado en el diccionario. \n Revise la hoja {} para ver cuáles registros tienen este problema'.format(nombre_hoja_resultados)
    return mensaje


def unicidad(fecha_corte, df_datos, llaves_primarias, columnas_dicc):
    # Leer archivo resumen
    with pd.ExcelFile("Resultados_Calidad_" + fecha_corte + ".xlsx") as xls:
        df_resumen_general = pd.read_excel(xls, sheet_name="Resumen general")
        df_resumen_campos = pd.read_excel(xls, sheet_name="Resumen campos")

    cols_datos = list(df_datos.columns)
    cols_coinciden = [x for x in columnas_dicc if x in cols_datos]
    nobs = len(df_datos) # Número total de registros para posteriormente calcular porcentaje
    campos_no_llaves = [x for x in cols_coinciden if x not in llaves_primarias] # Campos que no son llaves

    # Encontrar cuáles registros están duplicados solo considerando los IDs / llaves primarias
    duplicados_id = df_datos.duplicated(subset=llaves_primarias, keep=False)

    # Encontrar cuáles registros están duplicados considerando todos los demás campos diferentes a los IDs / llaves primarias
    duplicados_datos = df_datos.duplicated(subset=campos_no_llaves, keep=False)

    # Conteo de registros duplicados para evaluar si se genera un archivo de reporte o no
    conteo_dup_id = duplicados_id.sum()
    conteo_dup_datos = duplicados_datos.sum()

    registro_resumen_general = {"Nombre indicador": ["Fase 3. Calidad. Unicidad. Registros/Filas con IDs/llaves primarias duplicadas", "Fase 3. Calidad. Unicidad. Registros/Filas duplicados en campos diferentes a IDs/llaves primarias"],  
                "Cantidad en el corte " + fecha_corte: [conteo_dup_id, conteo_dup_datos],
                "Porcentaje en el corte " + fecha_corte: ["{:.2%}".format((conteo_dup_id/nobs)), "{:.2%}".format((conteo_dup_datos/nobs))]
                }
    df_resumen_general = pd.concat([df_resumen_general, pd.DataFrame(registro_resumen_general)], axis=0, ignore_index=True)

    with pd.ExcelWriter("Resultados_Calidad_" + fecha_corte + ".xlsx", \
        mode="a", if_sheet_exists="replace") as writer:
            df_resumen_general.to_excel(writer, sheet_name="Resumen general", index=False)

    nombre_hoja_resultados_id = "3.Unicidad_idDuplicados"
    nombre_hoja_resultados_reg = "3.Unicidad_RegistrosDuplicados"
    # Si no se identifican duplicados, no generar hoja adicional 
    if conteo_dup_id == 0 and conteo_dup_datos==0:
        mensaje = "No se encontraron duplicados por ID / llaves primarias o por otros campos"
    
    # Si se identifican duplicados, generar archivo y mensajes correspondientes
    if conteo_dup_id > 0:
        mensaje1 = "Hay {} registros con llaves/ID duplicados, correspondientes al {:.2%} del total de registros \n".format(conteo_dup_id, conteo_dup_id/nobs)
        listado_dup_id = df_datos[duplicados_id][llaves_primarias].sort_values(by=llaves_primarias)
        with pd.ExcelWriter("Resultados_Calidad_" + fecha_corte + ".xlsx", \
            mode="a", if_sheet_exists="replace") as writer:
                listado_dup_id.to_excel(writer, sheet_name=nombre_hoja_resultados_id, index=False)
    else:
        mensaje1 = "No se encontraron registros con llaves/ID duplicados. \n"
    
    if conteo_dup_datos > 0:
        mensaje2 = "Hay {} registros duplicados por campos diferentes a las llaves/ID, correspondientes al {:.2%} del total de registros.".format(conteo_dup_datos, conteo_dup_datos/nobs)
        listado_dup_datos = df_datos[duplicados_datos].sort_values(by=campos_no_llaves)
        with pd.ExcelWriter("Resultados_Calidad_" + fecha_corte + ".xlsx", \
            mode="a", if_sheet_exists="replace") as writer:
                listado_dup_datos.to_excel(writer, sheet_name=nombre_hoja_resultados_reg, index=False)
    else:
        mensaje2 = "No se encontraron registros duplicados por campos diferentes a las llaves/ID"
    
    mensaje = mensaje1 + mensaje2

    return mensaje