"""
Enunciado:
En este ejercicio aprenderás a utilizar SQLAlchemy como ORM (Object-Relational Mapper)
independiente de cualquier framework web. SQLAlchemy es una biblioteca potente
que permite trabajar con bases de datos relacionales utilizando objetos de Python.

Tarea:
Implementa un sistema simple de gestión de biblioteca utilizando SQLAlchemy para:
1. Crear modelos para Libros y Autores con una relación entre ellos
2. Realizar operaciones CRUD (Crear, Leer, Actualizar, Eliminar)
3. Realizar consultas básicas y avanzadas

Este ejercicio se enfoca en SQLAlchemy Core y ORM sin depender de Flask u otro framework web.
"""

from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, joinedload

# Crea el motor de base de datos (usamos SQLite en memoria para simplificar)
engine = create_engine('sqlite:///:memory:', echo=True)

# Crea la clase Base para los modelos declarativos
Base = declarative_base()


# Define aquí tus modelos
# Debes crear al menos:
# 1. Un modelo Author (autor) con campos id, name (nombre) y una relación con Book
# 2. Un modelo Book (libro) con campos id, title (título), year (año, opcional) y una relación con Author

class Author(Base):
     # Define la tabla 'authors' con:
    __tablename__ = "authors"

    # - id: clave primaria autoincremental
    id = Column(Integer, primary_key=True, autoincrement=True)

    # - name: nombre del autor (obligatorio)
    name = Column(String, nullable=False)

    # - Una relación con los libros (books) usando relationship
    books = relationship("Book", back_populates="author", cascade="all, delete")



class Book(Base):
    # Define la tabla 'books' con:
    __tablename__ = "books"

    # - id: clave primaria autoincremental
    id = Column(Integer, primary_key=True, autoincrement=True)

    # - title: título del libro (obligatorio)
    title = Column(String, nullable=False)

    # - year: año de publicación (opcional)
    year = Column(Integer)

    # - author_id: clave foránea que relaciona con la tabla 'authors'
    author_id = Column(Integer, ForeignKey("authors.id"))

    # - Una relación con el autor usando relationship
    author = relationship("Author", back_populates="books")


# Función para configurar la base de datos
def setup_database():
    """Configura la base de datos y crea las tablas"""
    # Implementa la creación de tablas en la base de datos usando Base.metadata.create_all()
    Base.metadata.create_all(engine)


# Función para crear datos de ejemplo
def create_sample_data(session):
    """Crea datos de ejemplo en la base de datos"""
    # Crea al menos dos autores
    a1 = Author(name="Autor A")
    a2 = Author(name="Autor B")

    # Crea al menos tres libros asociados a los autores
    b1 = Book(title="Libro 1", year=1990, author=a1)
    b2 = Book(title="Libro 2", year=1995, author=a1)
    b3 = Book(title="Libro 3", year=2000, author=a2)

    # Añade todos los objetos a la sesión y haz commit
    session.add_all([a1, a2, b1, b2, b3])
    session.commit()


# Funciones para operaciones CRUD
def create_book(session, title, author_name, year=None):
    """
    Crea un nuevo libro con su autor
    Si el autor ya existe, se utiliza el existente
    """
    # Busca si ya existe un autor con ese nombre
    author = session.query(Author).filter_by(name=author_name).first()

    # Si no existe, crea un nuevo autor
    if not author:
        author = Author(name=author_name)
        session.add(author)
        session.commit()

    # Crea un nuevo libro asociado al autor
    book = Book(title=title, year=year, author=author)

    # Añade y haz commit a la sesión
    session.add(book)
    session.commit()

    # Retorna el libro creado
    return book



def get_all_books(session):
    """Obtiene todos los libros con sus autores"""
    # Consulta todos los libros y carga también los autores (joinedload)
    return session.query(Book).options(joinedload(Book.author)).all()


def get_book_by_id(session, book_id):
    """Obtiene un libro específico por su ID"""
    # Busca un libro por su ID y retórnalo
    # Si no existe, retorna None
    return session.query(Book).filter_by(id=book_id).first()


def update_book(session, book_id, new_title=None, new_year=None):
    """Actualiza la información de un libro existente"""
    # Busca el libro por ID
    book = session.query(Book).filter_by(id=book_id).first()

    # Si existe, actualiza los campos que tienen nuevos valores
    if not book:
        return None

    if new_title:
        book.title = new_title
    if new_year:
        book.year = new_year

    # Haz commit a la sesión
    session.commit()

    # Retorna el libro actualizado o None si no existe
    return book



def delete_book(session, book_id):
    """Elimina un libro de la base de datos"""
    # Busca el libro por ID
    book = session.query(Book).filter_by(id=book_id).first()

    # Si existe, elimínalo y haz commit
    if book:
        session.delete(book)
        session.commit()


def find_books_by_author(session, author_name):
    """Busca libros por el nombre del autor"""
     # Consulta los libros uniendo (join) con la tabla de autores
    # Filtra por el nombre del autor
    # Retorna la lista de libros
    return (
        session.query(Book)
        .join(Author)
        .filter(Author.name == author_name)
        .all()
    )



# Función principal para demostrar el uso de SQLAlchemy
def main():
    """Función principal que demuestra el uso de SQLAlchemy"""
    # Crea un motor y una sesión
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Configura la base de datos
    setup_database()
    
    try:
        # Crea datos de ejemplo
        create_sample_data(session)
        
        # Demuestra las operaciones CRUD
        print("\n--- Todos los libros ---")
        books = get_all_books(session)
        for book in books:
            print(f"Libro: {book.title}, Año: {book.year}, Autor: {book.author.name}")
        
        print("\n--- Crear un nuevo libro ---")
        new_book = create_book(session, "Nuevo libro de ejemplo", "Autor de Prueba", 2025)
        print(f"Libro creado: {new_book.title} por {new_book.author.name}")
        
        print("\n--- Buscar libro por ID ---")
        book = get_book_by_id(session, 1)
        if book:
            print(f"Libro encontrado: {book.title} por {book.author.name}")
        
        print("\n--- Actualizar libro ---")
        updated_book = update_book(session, 1, new_title="Título Actualizado", new_year=2026)
        if updated_book:
            print(f"Libro actualizado: {updated_book.title}, Año: {updated_book.year}")
        
        print("\n--- Buscar libros por autor ---")
        author_books = find_books_by_author(session, "Autor de Prueba")
        for book in author_books:
            print(f"Libro: {book.title}, Año: {book.year}")
        
        print("\n--- Eliminar libro ---")
        delete_book(session, 2)
        print("Libro eliminado. Lista actualizada de libros:")
        for book in get_all_books(session):
            print(f"Libro: {book.title}, Autor: {book.author.name}")
    finally:
        # Cierra la sesión
        session.close()


if __name__ == "__main__":
    main()
