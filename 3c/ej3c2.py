"""
Enunciado:
En este ejercicio, implementarás un sistema de autenticación básico utilizando JSON Web Tokens (JWT)
para una API REST. JWT es un estándar abierto (RFC 7519) que define una forma compacta y
autónoma para transmitir información entre partes como un objeto JSON.

Tareas:
1. Implementar autenticación mediante JWT
2. Proteger la ruta del secreto para que solo usuarios autenticados puedan acceder
3. Manejar errores de autenticación y expiración de tokens con códigos HTTP apropiados

Esta versión utiliza Flask para crear la API REST y PyJWT para trabajar con tokens JWT.
"""

import datetime
import token
import jwt
from flask import Flask, jsonify, request
from functools import wraps

# Configuración JWT
JWT_SECRET_KEY = "clave_secreta_jwt_para_firmar_tokens"  # En producción, usar una clave segura
JWT_EXPIRATION_DELTA = datetime.timedelta(hours=1)  # Tiempo de expiración del token

# Credenciales de usuario fijas (en una aplicación real estarían en una base de datos)
USER_CREDENTIALS = {
    "usuario_demo": "password123"
}

def generate_jwt_token(username):
    """
    Genera un token JWT para un usuario

    Args:
        username: Nombre de usuario

    Returns:
        str: Token JWT generado
    """
   
    # Use timezone-aware UTC datetimes to avoid deprecation warnings
    now = datetime.datetime.now(datetime.timezone.utc)
    # Set iat a 1 segundo en el pasado para evitar errores por pequeña deriva de tiempo
    iat = int(now.timestamp()) - 1
    exp_dt = now + JWT_EXPIRATION_DELTA
    exp = int(exp_dt.timestamp())
    payload = {
        'sub': username,
        'iat': iat,
        'exp': exp
    }
    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm='HS256')
    # jwt.encode may return bytes in some versions
    if isinstance(token, bytes):
        token = token.decode('utf-8')
    return token

def jwt_required(func):
    """
    Decorador que verifica la autenticación mediante token JWT

    Para usar este decorador, añade @jwt_required a las funciones que requieran autenticación.
    El token debe enviarse en la cabecera 'Authorization' con formato: 'Bearer TOKEN'

    Args:
        func: Función a decorar

    Returns:
        Function: Función decorada con verificación de autenticación JWT
    """
    @wraps(func)
    def decorated_function(*args, **kwargs):
        """
        Implementa esta función para:
        1. Extraer el token JWT de la cabecera 'Authorization'
        2. Verificar que el formato sea 'Bearer TOKEN'
        3. Decodificar y verificar el token usando jwt.decode()
        4. Si hay algún error (token expirado, inválido, etc.), devolver un error apropiado
        """
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify({'error': 'Token inválido '}), 401

        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != 'bearer':
            return jsonify({'error': 'Token inválido '}), 401

        token = parts[1]
        try:
            decoded = jwt.decode(token, JWT_SECRET_KEY, algorithms=['HS256'])
            # attach user info if needed
            request.jwt_payload = decoded
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expirado'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Token inválido o ausente'}), 401

        return func(*args, **kwargs)
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

    @app.route('/api/auth/login', methods=['POST'])
    def login():
        """
        Inicia sesión y genera un token JWT

        Recibe username y password en un JSON, verifica las credenciales,
        y genera un nuevo token JWT si son correctas.

        Ejemplo de uso:
            POST /api/auth/login
            Content-Type: application/json

            {
                "username": "usuario_demo",
                "password": "password123"
            }

        Respuesta exitosa:
            Status: 200 OK
            {
                "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6...",
                "expires_at": "2023-01-01T12:00:00Z"
            }

        Respuesta de error:
            Status: 401 Unauthorized
            {
                "error": "Credenciales inválidas"
            }
        """
        
        
        data = request.get_json() or {}
        username = data.get('username')
        password = data.get('password')

        if not username or not password:
            return jsonify({'error': 'Credenciales inválidas'}), 401

        expected = USER_CREDENTIALS.get(username)
        if expected is None or expected != password:
            return jsonify({'error': 'Credenciales inválidas'}), 401

        token = generate_jwt_token(username)
    
        # Calcular expires_at ISO format (sin usar métodos deprecados)
        exp_ts = jwt.decode(token, JWT_SECRET_KEY, algorithms=['HS256'], options={"verify_exp": False}).get('exp')
    
        expires_at = datetime.datetime.fromtimestamp(exp_ts, tz=datetime.timezone.utc).isoformat()
        return jsonify({'token': token, 'expires_at': expires_at})

    @app.route('/api/secret', methods=['GET'])
    @jwt_required
    def protected_secret():
        """
        Endpoint protegido que requiere autenticación JWT y devuelve un mensaje secreto

        Este endpoint requiere autenticación mediante el decorador @jwt_required.

        Ejemplo de uso:
            GET /api/secret
            Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6...

        Respuesta exitosa:
            Status: 200 OK
            {
                "message": "¡Has accedido al secreto con JWT!",
                "secret": "La respuesta a la vida, el universo y todo lo demás es 42"
            }

        Respuesta de error:
            Status: 401 Unauthorized
            {
                "error": "Token inválido o ausente"
            }
        """
        return jsonify({
            'message': '¡Has accedido al secreto con JWT!',
            'secret': 'La respuesta a la vida, el universo y todo lo demás es 42'
        })

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
