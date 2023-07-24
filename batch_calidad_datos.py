
import pandas as pd
from datetime import date
from funciones_calidad import *
from utils.utils import *
from utils.log import *

def batch_calidad_datos(
    archivo_datos, archivo_datos_dic, 
    hoja_datos=0, hoja_datos_dic=0, 
    corte=str(date.today()), nombre_log_datos="Log_calidad", 
    rango_columnas=None, inicial_fila=None,
    rango_columnas_dic=None, inicial_fila_dic=None
):
    """# 1. Lectura de datos y diccionario"""

    df_log = creacion_log(ruta_archivo_log=nombre_log_datos)

    # Inicio de log lectura de datos
    df_log = inicio_log(df_log, \
        fase = 'Fase 1. Lectura de datos y diccionario',
        accion = 'Lectura de datos y verificación de columnas',
        descripcion = 'Se leen los datos, se verifica que no haya columnas con el mismo nombre y se da una sugerencia para estandarizar los nombres de las variables'
        )

    # Leer datos
    datos, resultado_paralog  = lectura_datos(corte, archivo_datos, hoja_datos, rango_columnas, inicial_fila)

    # Fin de log
    df_log = fin_log(df_log, ruta_archivo_log=nombre_log_datos, \
        mensaje= resultado_paralog
        )

    # Inicio de log lectura de diccionario
    df_log = inicio_log(df_log, \
        fase = 'Fase 1. Lectura de datos y diccionario',
        accion = 'Lectura de diccionario',
        descripcion = 'Se lee el archivo que contiene el diccionario y se generan elementos que permitiran verificar la estructura y validez de los datos acorde al diccionario'
        )

    # Leer diccionario y generar elementos necesarios para luego verificar la estructura y validez de los datos
    diccionario, columnas_dicc, llave_primaria, campos_obligatorios, \
        tipo_dominio, dominio, tipo_dato, longitud, \
        resultado_paralog  = lectura_diccionario(
        archivo_datos_dic, hoja_datos_dic, rango_columnas_dic, inicial_fila_dic)

    # Fin de log
    df_log = fin_log(df_log, ruta_archivo_log=nombre_log_datos, \
        mensaje = resultado_paralog
        )

    """# 2. Revisión de estructura"""

    # Inicio de log
    df_log = inicio_log(df_log, \
        fase = 'Fase 2. Revisión de estructura de los datos',
        accion = 'Validar la correspondencia entre las variables del diccionario y los datos',
        descripcion = 'Contar cuántas variables hay en el diccionario, cuántas en los datos y conocer cuáles no coinciden entre ambos'
        )

    # Leer diccionario y generar elementos necesarios para luego verificar la estructura y validez de los datos
    resultado_paralog  = validar_variables(corte, datos, columnas_dicc, parar_si_errores=False)

    # Fin de log
    df_log = fin_log(df_log, ruta_archivo_log=nombre_log_datos, \
        mensaje = resultado_paralog
        )

    # Inicio de log
    df_log = inicio_log(df_log, \
        fase = 'Fase 2. Revisión de estructura de los datos',
        accion = 'Asignación de tipo de dato',
        descripcion = 'Asignar a los datos el tipo definido en el diccionario (entero, numérico, cadena)'
        )

    # Leer diccionario y generar elementos necesarios para luego verificar la estructura y validez de los datos
    datos, resultado_paralog  = asignar_tipo(datos, tipo_dato)

    # Fin de log
    df_log = fin_log(df_log, ruta_archivo_log=nombre_log_datos, \
        mensaje = resultado_paralog
        )

    """# 3. Calidad de los datos"""

    # Inicio de log
    df_log = inicio_log(df_log, \
        fase = 'Fase 3. Calidad de los datos',
        accion = 'Completitud de campos obligatorios y no obligatorios',
        descripcion = 'Se verifica que los campos marcados como obligatorios en el diccionario tengan registros en todas sus filas'
        )

    # Leer diccionario y generar elementos necesarios para luego verificar la estructura y validez de los datos
    resultado_paralog  = validar_obligatorios(corte, datos, llave_primaria, columnas_dicc, campos_obligatorios)

    # Fin de log
    df_log = fin_log(df_log, ruta_archivo_log=nombre_log_datos, \
        mensaje = resultado_paralog
        )

    # Inicio de log
    df_log = inicio_log(df_log, \
        fase = 'Fase 3. Calidad de los datos',
        accion = 'Validez de los datos según dominio',
        descripcion = 'Validez de los datos según el dominio definido en el diccionario, verificando si los registros en los datos están por fuera del dominio'
        )

    # Leer diccionario y generar elementos necesarios para luego verificar la estructura y validez de los datos
    resultado_paralog  = validar_dominio(corte, datos, llave_primaria, tipo_dominio, dominio)

    # Fin de log
    df_log = fin_log(df_log, ruta_archivo_log=nombre_log_datos, \
        mensaje = resultado_paralog
        )

    # Inicio de log
    df_log = inicio_log(df_log, \
        fase = 'Fase 3. Calidad de los datos',
        accion = 'Validez de los datos según longitud',
        descripcion = 'Validez de los datos según la longitud definida en el diccionario, verificando si los registros tienen una longitud de caracteres mayor a lo estipulado'
        )

    # Leer diccionario y generar elementos necesarios para luego verificar la estructura y validez de los datos
    resultado_paralog  = validar_longitud(corte, datos, llave_primaria, longitud)

    # Fin de log
    df_log = fin_log(df_log, ruta_archivo_log=nombre_log_datos, \
        mensaje = resultado_paralog
        )

    # Inicio de log
    df_log = inicio_log(df_log, \
        fase = 'Fase 3. Calidad de los datos',
        accion = 'Unicidad de los registros',
        descripcion = 'Validar que el campo de ID no tenga valores repetidos, y que los registros sin considerar el ID tampoco estén duplicados'
        )

    # Leer diccionario y generar elementos necesarios para luego verificar la estructura y validez de los datos
    resultado_paralog  = unicidad(corte, datos, llave_primaria, columnas_dicc)

    # Fin de log
    df_log = fin_log(df_log, ruta_archivo_log=nombre_log_datos, \
        mensaje = resultado_paralog
        )