"""
Enunciado:
Desarrolla una API REST utilizando Flask que permita realizar operaciones básicas sobre una biblioteca
con dos modelos relacionados: Autores y Libros.
La API debe exponer los siguientes endpoints:

Autores:
1. `GET /authors`: Devuelve la lista completa de autores.
2. `POST /authors`: Agrega un nuevo autor. El cuerpo de la solicitud debe incluir un JSON con el campo "name".
3. `GET /authors/<author_id>`: Obtiene los detalles de un autor específico y su lista de libros.

Libros:
1. `GET /books`: Devuelve la lista completa de libros.
2. `POST /books`: Agrega un nuevo libro. El cuerpo de la solicitud debe incluir JSON con campos "title", "author_id", y "year" (opcional).
3. `DELETE /books/<book_id>`: Elimina un libro específico por su ID.
4. `PUT /books/<book_id>`: Actualiza la información de un libro existente. El cuerpo puede incluir "title" y/o "year".

Esta versión utiliza Flask-SQLAlchemy como ORM para persistir los datos en una base de datos SQLite.
"""

from flask import Flask, abort, jsonify, request
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# Define aquí tus modelos
# Usa los mismos modelos que en el ejercicio anterior: Author y Book

class Author(db.Model):
    """
    Modelo de autor usando SQLAlchemy ORM
    Debe tener: id, name y una relación con los libros
    """
    __tablename__ = 'authors'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String, nullable=False)

    books = db.relationship('Book', backref='author', cascade='all, delete-orphan')

    def to_dict(self):
        """Convierte el autor a un diccionario para la respuesta JSON"""

        return {
            'id': self.id,
            'name': self.name
        }

class Book(db.Model):
    """
    Modelo de libro usando SQLAlchemy ORM
    Debe tener: id, title, year (opcional), author_id y relación con el autor
    """
    __tablename__ = 'books'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    title = db.Column(db.String, nullable=False)
    year = db.Column(db.Integer, nullable=True)
    author_id = db.Column(db.Integer, db.ForeignKey('authors.id'), nullable=False)

    def to_dict(self):
        """Convierte el libro a un diccionario para la respuesta JSON"""
        
        return {
            'id': self.id,
            'title': self.title,
            'year': self.year,
            'author_id': self.author_id
        }

def create_app():
    """
    Crea y configura la aplicación Flask con SQLAlchemy
    """
    app = Flask(__name__)
    
    # Configuración de la base de datos SQLite en memoria
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Inicializa la base de datos con la aplicación
    db.init_app(app)
    
    # Crea todas las tablas en la base de datos
    with app.app_context():
        db.create_all()
    
    
    # Endpoints de Autores
    @app.route('/authors', methods=['GET'])
    def get_authors():
        """
        Devuelve la lista completa de autores
        """
        authors = Author.query.order_by(Author.id).all()
        return jsonify([a.to_dict() for a in authors])

    @app.route('/authors', methods=['POST'])
    def add_author():
        """
        Agrega un nuevo autor
        El cuerpo de la solicitud debe incluir un JSON con el campo "name"
        """
        data = request.get_json() or {}
        name = data.get('name')
        if not name:
            return jsonify({'error': 'Missing name'}), 400

        author = Author(name=name)
        db.session.add(author)
        db.session.commit()
        return jsonify(author.to_dict()), 201

    @app.route('/authors/<int:author_id>', methods=['GET'])
    def get_author(author_id):
        """
        Obtiene los detalles de un autor específico y su lista de libros
        """
        author = Author.query.get_or_404(author_id)
        # Obtener libros del autor
        books = [b.to_dict() for b in Book.query.filter_by(author_id=author.id).order_by(Book.id).all()]
        result = author.to_dict()
        result['books'] = books
        return jsonify(result)


    # Endpoints de Libros
    @app.route('/books', methods=['GET'])
    def get_books():
        """
        Devuelve la lista completa de libros
        """
        books = Book.query.order_by(Book.id).all()
        return jsonify([b.to_dict() for b in books])

    @app.route('/books', methods=['POST'])
    def add_book():
        """
        Agrega un nuevo libro
        El cuerpo de la solicitud debe incluir JSON con campos "title", "author_id", y "year" (opcional)
        """
        data = request.get_json() or {}
        title = data.get('title')
        author_id = data.get('author_id')
        year = data.get('year')

        if not title or author_id is None:
            return jsonify({'error': 'Missing title or author_id'}), 400

        # Verificar que el autor existe
        author = Author.query.get(author_id)
        if not author:
            return jsonify({'error': 'Author not found'}), 400

        book = Book(title=title, year=year, author_id=author_id)
        db.session.add(book)
        db.session.commit()
        return jsonify(book.to_dict()), 201
    
    @app.route('/books/<int:book_id>', methods=['GET'])
    def get_book(book_id):
        """
        Obtiene un libro específico por su ID
        """
        book = Book.query.get_or_404(book_id)
        return jsonify(book.to_dict())
    
    @app.route('/books/<int:book_id>', methods=['DELETE'])
    def delete_book(book_id):
        """
        Elimina un libro específico por su ID
        """
        book = Book.query.get(book_id)
        if not book:
            return jsonify({'error': 'Not found'}), 404
        db.session.delete(book)
        db.session.commit()
        return ('', 204)

    @app.route('/books/<int:book_id>', methods=['PUT'])
    def update_book(book_id):
        """
        Actualiza la información de un libro existente
        El cuerpo puede incluir "title" y/o "year"
        """
        data = request.get_json() or {}
        book = Book.query.get(book_id)
        if not book:
            return jsonify({'error': 'Not found'}), 404

        title = data.get('title')
        year = data.get('year')

        if title is not None:
            book.title = title
        if year is not None:
            book.year = year

        db.session.add(book)
        db.session.commit()
        return jsonify(book.to_dict())

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
