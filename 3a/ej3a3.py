"""
Enunciado:
En este ejercicio aprenderás a trabajar con bases de datos SQLite existentes.
Aprenderás a:
1. Conectar a una base de datos SQLite existente
2. Convertir datos de SQLite a formatos compatibles con JSON
3. Extraer datos de SQLite a pandas DataFrame

El archivo ventas_comerciales.db contiene datos de ventas con tablas relacionadas
que incluyen productos, vendedores, regiones y ventas. Debes analizar estos datos
usando diferentes técnicas.
"""

import sqlite3
import pandas as pd
import os
import json
from typing import List, Dict, Any, Optional, Tuple, Union

# Ruta a la base de datos SQLite
DB_PATH = os.path.join(os.path.dirname(__file__), 'ventas_comerciales.db')

def conectar_bd() -> sqlite3.Connection:
    """
    Conecta a una base de datos SQLite existente

    Returns:
        sqlite3.Connection: Objeto de conexión a la base de datos SQLite
    """
    # Implementa aquí la conexión a la base de datos:
    # 1. Verifica que el archivo de base de datos existe
    # 2. Conecta a la base de datos
    # 3. Configura la conexión para que devuelva las filas como diccionarios (opcional)
    # 4. Retorna la conexión
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"La base de datos no existe en la ruta: {DB_PATH}")
    conexion = sqlite3.connect(DB_PATH)
    conexion.row_factory = sqlite3.Row
    return conexion

def convertir_a_json(conexion: sqlite3.Connection) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convierte los datos de la base de datos en un objeto compatible con JSON

    Args:
        conexion (sqlite3.Connection): Conexión a la base de datos SQLite

    Returns:
        Dict[str, List[Dict[str, Any]]]: Diccionario con todas las tablas y sus registros
        en formato JSON-serializable
    """
    # Implementa aquí la conversión de datos a formato JSON:
    # 1. Crea un diccionario vacío para almacenar el resultado
    # 2. Obtén la lista de tablas de la base de datos
    # 3. Para cada tabla:
    #    a. Ejecuta una consulta SELECT * FROM tabla
    #    b. Obtén los nombres de las columnas
    #    c. Convierte cada fila a un diccionario (clave: nombre columna, valor: valor celda)
    #    d. Añade el diccionario a una lista para esa tabla
    # 4. Retorna el diccionario completo con todas las tablas
    resultado: Dict[str, List[Dict[str, Any]]] = {}
    cursor = conexion.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = [t[0] for t in cursor.fetchall()]

    for tabla in tablas:
        # Obtener filas
        cursor.execute(f"SELECT * FROM {tabla}")
        filas = cursor.fetchall()

        # Obtener nombres de columnas
        colnames = [desc[0] for desc in cursor.description] if cursor.description else []

        registros: List[Dict[str, Any]] = []
        for fila in filas:
            # fila es una tupla; convertir a dict
            registro = {col: fila[idx] for idx, col in enumerate(colnames)}
            registros.append(registro)

        resultado[tabla] = registros

    return resultado


def convertir_a_dataframes(conexion: sqlite3.Connection) -> Dict[str, pd.DataFrame]:
    """
    Extrae los datos de la base de datos a DataFrames de pandas

    Args:
        conexion (sqlite3.Connection): Conexión a la base de datos SQLite

    Returns:
        Dict[str, pd.DataFrame]: Diccionario con DataFrames para cada tabla y para
        consultas combinadas relevantes
    """
    # Implementa aquí la extracción de datos a DataFrames:
    # 1. Crea un diccionario vacío para los DataFrames
    # 2. Obtén la lista de tablas de la base de datos
    # 3. Para cada tabla, crea un DataFrame usando pd.read_sql_query
    # 4. Añade consultas JOIN para relaciones importantes:
    #    - Ventas con información de productos
    #    - Ventas con información de vendedores
    #    - Vendedores con regiones
    # 5. Retorna el diccionario con todos los DataFrames
    dfs: Dict[str, pd.DataFrame] = {}
    cursor = conexion.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = [t[0] for t in cursor.fetchall()]

    for tabla in tablas:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {tabla}", conexion)
        except Exception:
            df = pd.DataFrame()
        dfs[tabla] = df

    # Añadir consultas combinadas relevantes
    try:
        dfs['ventas_productos'] = pd.read_sql_query(
            """
            SELECT ventas.*, productos.nombre AS producto_nombre, productos.categoria, productos.precio_unitario
            FROM ventas
            JOIN productos ON ventas.producto_id = productos.id
            """,
            conexion
        )
    except Exception:
        pass

    try:
        dfs['ventas_vendedores'] = pd.read_sql_query(
            """
            SELECT ventas.*, vendedores.nombre AS vendedor_nombre, vendedores.region_id
            FROM ventas
            JOIN vendedores ON ventas.vendedor_id = vendedores.id
            """,
            conexion
        )
    except Exception:
        pass

    try:
        dfs['vendedores_regiones'] = pd.read_sql_query(
            """
            SELECT vendedores.*, regiones.nombre AS region_nombre, regiones.pais
            FROM vendedores
            JOIN regiones ON vendedores.region_id = regiones.id
            """,
            conexion
        )
    except Exception:
        pass

    return dfs


if __name__ == "__main__":
    try:
        # Conectar a la base de datos existente
        print("Conectando a la base de datos...")
        conexion = conectar_bd()
        print("Conexión establecida correctamente.")

        # Verificar la conexión mostrando las tablas disponibles
        cursor = conexion.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablas = cursor.fetchall()
        print(f"\nTablas en la base de datos: {[t[0] for t in tablas]}")

        # Conversión a JSON
        print("\n--- Convertir datos a formato JSON ---")
        datos_json = convertir_a_json(conexion)
        print("Estructura JSON (ejemplo de una tabla):")
        if datos_json:
            # Muestra un ejemplo de la primera tabla encontrada
            primera_tabla = list(datos_json.keys())[0]
            print(f"Tabla: {primera_tabla}")
            if datos_json[primera_tabla]:
                print(f"Primer registro: {datos_json[primera_tabla][0]}")

            # Opcional: guardar los datos en un archivo JSON
            # ruta_json = os.path.join(os.path.dirname(__file__), 'ventas_comerciales.json')
            # with open(ruta_json, 'w', encoding='utf-8') as f:
            #     json.dump(datos_json, f, ensure_ascii=False, indent=2)
            # print(f"Datos guardados en {ruta_json}")

        # Conversión a DataFrames de pandas
        print("\n--- Convertir datos a DataFrames de pandas ---")
        dataframes = convertir_a_dataframes(conexion)
        if dataframes:
            print(f"Se han creado {len(dataframes)} DataFrames:")
            for nombre, df in dataframes.items():
                print(f"- {nombre}: {len(df)} filas x {len(df.columns)} columnas")
                print(f"  Columnas: {', '.join(df.columns.tolist())}")
                print(f"  Vista previa:\n{df.head(2)}\n")

    except sqlite3.Error as e:
        print(f"Error de SQLite: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conexion' in locals() and conexion:
            conexion.close()
            print("\nConexión cerrada.")
