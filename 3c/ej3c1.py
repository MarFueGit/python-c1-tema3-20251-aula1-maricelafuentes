"""
Enunciado:
En este ejercicio, implementarás un sistema de autenticación básico para una API REST
utilizando un token simple enviado en las cabeceras HTTP. Este método es simple pero
ilustra los conceptos fundamentales de autenticación en APIs.

Tareas:
1. Implementar autenticación mediante token simple en las cabeceras HTTP
2. Proteger la ruta del secreto para que solo usuarios autenticados puedan acceder
3. Manejar errores de autenticación con códigos HTTP apropiados

Esta versión utiliza Flask para crear la API REST.
"""

from flask import Flask, jsonify, request, g
from functools import wraps

# Token secreto predefinido (en una aplicación real, estos tokens estarían almacenados de forma segura)
API_TOKEN = "mi_token_secreto_1234"

def auth_required(func):
    """
    Decorador que verifica la autenticación mediante token en las cabeceras HTTP

    Para usar este decorador, añade @auth_required a las funciones que requieran autenticación.
    El token debe enviarse en la cabecera 'Authorization' con formato: 'Bearer TOKEN'

    Args:
        func: Función a decorar

    Returns:
        Function: Función decorada con verificación de autenticación
    """
    @wraps(func)
    def decorated_function(*args, **kwargs):
        """
        Implementa esta función para:
        1. Extraer el token de la cabecera 'Authorization'
        2. Verificar que el formato sea 'Bearer TOKEN'
        3. Comprobar que el token coincide con API_TOKEN
        4. Si coincide, ejecutar la función original
        5. Si no coincide o hay algún error, devolver un error 401 Unauthorized
        """
        # 1. Extraer el token de la cabecera 'Authorization'
        auth_header = request.headers.get('Authorization')
        
        # Verificar si la cabecera existe
        if not auth_header:
            return jsonify({
                'error': 'Cabecera de autorización faltante',
                'message': 'Se requiere el token de autenticación'
            }), 401
        
        # 2. Verificar que el formato sea 'Bearer TOKEN'
        try:
            # Dividir el header en dos partes: 'Bearer' y el token
            parts = auth_header.split()
            
            if len(parts) != 2 or parts[0].lower() != 'bearer':
                return jsonify({
                    'error': 'Formato de autorización inválido',
                    'message': 'El formato debe ser: Bearer TOKEN'
                }), 401
            
            token = parts[1]
            
            # 3. Comprobar que el token coincide con API_TOKEN
            if token != API_TOKEN:
                return jsonify({
                    'error': 'Token inválido',
                    'message': 'El token proporcionado no es válido'
                }), 401
            
            # 4. Si coincide, ejecutar la función original
            return func(*args, **kwargs)
            
        except Exception :
            # 5. Si hay algún error, devolver un error 401 Unauthorized
            return jsonify({
                'error': 'Error en la autenticación',
                'message': 'Error procesando la solicitud de autenticación'
            }), 401
        
    return decorated_function


def create_app():
    """
    Crea y configura la aplicación Flask
    """
    app = Flask(__name__)

    @app.route('/api/public', methods=['GET'])
    def public_endpoint():
        """
        Endpoint público que no requiere autenticación

        Ejemplo de uso:
            GET /api/public

        Respuesta:
            Status: 200 OK
            {
                "message": "Este es un endpoint público, cualquiera puede acceder"
            }
        """
        return jsonify({
            "message": "Este es un endpoint público, cualquiera puede acceder"
        })

    @app.route('/api/secret', methods=['GET'])
    @auth_required
    def protected_secret():
        """
        Endpoint protegido que requiere autenticación y devuelve un mensaje secreto

        Este endpoint requiere autenticación mediante el decorador @auth_required.

        Ejemplo de uso:
            GET /api/secret
            Authorization: Bearer mi_token_secreto_1234

        Respuesta exitosa:
            Status: 200 OK
            {
                "message": "¡Has accedido al secreto!",
                "secret": "La respuesta a la vida, el universo y todo lo demás es 42"
            }

        Respuesta de error:
            Status: 401 Unauthorized
            {
                "error": "Token inválido o ausente"
            }
        """
        return jsonify({
            "message": "¡Has accedido al secreto!",
            "secret": "La respuesta a la vida, el universo y todo lo demás es 42"
        })

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
