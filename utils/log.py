import pandas as pd
from pandas import Timestamp
from utils.utils import dif_tiempo_a_texto

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