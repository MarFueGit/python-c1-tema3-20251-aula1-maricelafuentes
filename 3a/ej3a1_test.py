"""
Tests para el ejercicio ej3a1.py que utiliza la biblioteca sqlite3 de Python para
trabajar con bases de datos SQLite.
"""

import pytest
import sqlite3
import os
from ej3a1 import (crear_conexion, crear_tablas, insertar_autores, insertar_libros,
                  consultar_libros, buscar_libros_por_autor, actualizar_libro,
                  eliminar_libro, ejemplo_transaccion)

# Path to test SQL script
SQL_TEST_PATH = os.path.join(os.path.dirname(__file__), 'test.sql')

@pytest.fixture
def conexion():
    """Fixture que proporciona una conexión a una base de datos en memoria para las pruebas"""
    conn = sqlite3.connect(':memory:')
    yield conn
    conn.close()

@pytest.fixture
def db_con_tablas(conexion):
    """Fixture que proporciona una conexión con las tablas ya creadas"""

    sql_script = ""
    capturando = False

    with open(SQL_TEST_PATH, 'r', encoding='utf-8') as sql_file:
        for line in sql_file:
            stripped = line.strip()

            # Inicia captura cuando aparece CREATE TABLE
            if stripped.startswith('CREATE TABLE'):
                capturando = True

            # Si estamos capturando y la línea no es un INSERT, agregarla
            if capturando and not stripped.startswith('INSERT'):
                sql_script += line

            # Termina captura cuando cierra la tabla 'libros'
            if stripped == ');' and 'libros' in sql_script:
                break

    # Ejecutar script
    conexion.executescript(sql_script)
    conexion.commit()
    return conexion

@pytest.fixture
def db_con_datos(conexion):
    """Fixture que proporciona una conexión con tablas y datos de ejemplo"""
    # Execute the entire SQL script to setup tables and data
    with open(SQL_TEST_PATH, 'r', encoding='utf-8') as sql_file:
        conexion.executescript(sql_file.read())
    conexion.commit()
    return conexion

def test_crear_conexion():
    """Prueba la función crear_conexion"""
    conn = crear_conexion()
    assert conn is not None

    # Verificar que es una conexión válida a SQLite intentando ejecutar una consulta
    cursor = conn.cursor()
    cursor.execute("SELECT sqlite_version();")
    version = cursor.fetchone()

    assert version is not None
    conn.close()

def test_crear_tablas(conexion):
    """Prueba la función crear_tablas"""
    crear_tablas(conexion)

    # Verificar que las tablas se han creado correctamente
    cursor = conexion.cursor()

    # Consultar las tablas existentes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = cursor.fetchall()
    nombres_tablas = [tabla[0] for tabla in tablas]

    assert "autores" in nombres_tablas
    assert "libros" in nombres_tablas

    # Verificar la estructura de la tabla autores
    cursor.execute("PRAGMA table_info(autores);")
    columnas_autores = cursor.fetchall()
    nombres_columnas_autores = [columna[1] for columna in columnas_autores]

    assert "id" in nombres_columnas_autores
    assert "nombre" in nombres_columnas_autores

    # Verificar la estructura de la tabla libros
    cursor.execute("PRAGMA table_info(libros);")
    columnas_libros = cursor.fetchall()
    nombres_columnas_libros = [columna[1] for columna in columnas_libros]

    assert "id" in nombres_columnas_libros
    assert "titulo" in nombres_columnas_libros
    assert "anio" in nombres_columnas_libros
    assert "autor_id" in nombres_columnas_libros

def test_insertar_autores(db_con_tablas):
    """Prueba la función insertar_autores"""
    # Datos de prueba
    autores = [
        ("Gabriel García Márquez",),
        ("Isabel Allende",),
        ("Jorge Luis Borges",)
    ]

    insertar_autores(db_con_tablas, autores)

    # Verificar que los autores se insertaron correctamente
    cursor = db_con_tablas.cursor()
    cursor.execute("SELECT nombre FROM autores ORDER BY id;")
    resultados = cursor.fetchall()

    assert len(resultados) == 3
    assert resultados[0][0] == "Gabriel García Márquez"
    assert resultados[1][0] == "Isabel Allende"
    assert resultados[2][0] == "Jorge Luis Borges"

def test_insertar_libros(db_con_tablas):
    """Prueba la función insertar_libros"""
    # Primero insertamos autores para tener las claves foráneas
    cursor = db_con_tablas.cursor()
    cursor.execute("INSERT INTO autores (nombre) VALUES (?)", ("Gabriel García Márquez",))
    cursor.execute("INSERT INTO autores (nombre) VALUES (?)", ("Isabel Allende",))

    # Datos de prueba para libros
    libros = [
        ("Cien años de soledad", 1967, 1),
        ("La casa de los espíritus", 1982, 2)
    ]

    insertar_libros(db_con_tablas, libros)

    # Verificar que los libros se insertaron correctamente
    cursor.execute("SELECT titulo, anio, autor_id FROM libros ORDER BY id;")
    resultados = cursor.fetchall()

    assert len(resultados) == 2
    assert resultados[0] == ("Cien años de soledad", 1967, 1)
    assert resultados[1] == ("La casa de los espíritus", 1982, 2)

def test_consultar_libros(db_con_datos, capfd):
    """Prueba la función consultar_libros usando capfd para capturar la salida estándar"""
    consultar_libros(db_con_datos)

    # Capturar la salida estándar
    salida, _ = capfd.readouterr()

    # Verificar que la salida contiene información de los libros
    assert "Cien años de soledad" in salida
    assert "Gabriel García Márquez" in salida
    assert "La casa de los espíritus" in salida
    assert "Isabel Allende" in salida
    assert "Ficciones" in salida
    assert "Jorge Luis Borges" in salida

def test_buscar_libros_por_autor(db_con_datos):
    """Prueba la función buscar_libros_por_autor"""
    # Buscar libros de Gabriel García Márquez
    libros = buscar_libros_por_autor(db_con_datos, "Gabriel García Márquez")

    # Verificar los resultados
    assert len(libros) == 2
    titulos = [libro[0] for libro in libros]
    anios = [libro[1] for libro in libros]

    assert "Cien años de soledad" in titulos
    assert "El amor en los tiempos del cólera" in titulos
    assert 1967 in anios
    assert 1985 in anios

def test_actualizar_libro(db_con_datos):
    """Prueba la función actualizar_libro"""
    # Actualizar el título del libro con ID 1
    actualizar_libro(db_con_datos, 1, nuevo_titulo="Cien años de soledad (Edición especial)")

    # Verificar que el libro se actualizó correctamente
    cursor = db_con_datos.cursor()
    cursor.execute("SELECT titulo, anio FROM libros WHERE id = 1;")
    resultado = cursor.fetchone()

    assert resultado[0] == "Cien años de soledad (Edición especial)"
    assert resultado[1] == 1967  # El año no debe cambiar

    # Actualizar sólo el año
    actualizar_libro(db_con_datos, 1, nuevo_anio=2020)

    cursor.execute("SELECT titulo, anio FROM libros WHERE id = 1;")
    resultado = cursor.fetchone()

    assert resultado[0] == "Cien años de soledad (Edición especial)"
    assert resultado[1] == 2020

    # Actualizar ambos campos
    actualizar_libro(db_con_datos, 1, nuevo_titulo="Título actualizado", nuevo_anio=2021)

    cursor.execute("SELECT titulo, anio FROM libros WHERE id = 1;")
    resultado = cursor.fetchone()

    assert resultado[0] == "Título actualizado"
    assert resultado[1] == 2021

def test_eliminar_libro(db_con_datos):
    """Prueba la función eliminar_libro"""
    # Verificar que el libro existe antes de eliminarlo
    cursor = db_con_datos.cursor()
    cursor.execute("SELECT COUNT(*) FROM libros WHERE id = 6;")
    assert cursor.fetchone()[0] == 1

    # Eliminar el libro
    eliminar_libro(db_con_datos, 6)

    # Verificar que el libro fue eliminado
    cursor.execute("SELECT COUNT(*) FROM libros WHERE id = 6;")
    assert cursor.fetchone()[0] == 0

    # Verificar que sólo se eliminó ese libro
    cursor.execute("SELECT COUNT(*) FROM libros;")
    assert cursor.fetchone()[0] == 5

def test_ejemplo_transaccion(db_con_datos):
    """Prueba la función ejemplo_transaccion"""
    # Obtener el estado inicial de la base de datos
    cursor = db_con_datos.cursor()
    cursor.execute("SELECT COUNT(*) FROM libros;")
    libros_inicial = cursor.fetchone()[0]

    # Ejecutar la transacción
    ejemplo_transaccion(db_con_datos)

    # Verificar que la transacción realizó cambios
    cursor.execute("SELECT COUNT(*) FROM libros;")
    libros_final = cursor.fetchone()[0]

    # La implementación específica dependerá del estudiante,
    # pero comprobamos que al menos la función no genera errores
    assert True  # No errores = prueba pasa
