import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote

class MySQLConnector:
    @staticmethod
    def get_connection(
        database: str,
        host: str = None,
        port: str = None,
        user: str = None,
        password: str = None,
    ):
        current_folder = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(current_folder, ".env")
        
        if not load_dotenv(env_path):
            print(f"⚠️ Advertencia: No se encontró .env en {env_path}")

        var_host = host or os.getenv("DB_HOST")
        var_port = port or os.getenv("DB_PORT")
        var_user = user or os.getenv("DB_USER")
        var_pass = quote(password or os.getenv("DB_PASSWORD"))

        # Validaciones
        if not all([var_host, var_port, var_user, var_pass]):
            missing = []
            if not var_host: missing.append("DB_HOST")
            if not var_port: missing.append("DB_PORT")
            if not var_user: missing.append("DB_USER")
            if not var_pass: missing.append("DB_PASSWORD")
            raise ValueError(f"Faltan variables de conexión: {', '.join(missing)}")

        connection_string = (
            f"mysql+mysqldb://{var_user}:{var_pass}@{var_host}:{var_port}/{database}"
        )

        try:
            engine = create_engine(
                connection_string,
                pool_recycle=9600,
                isolation_level="AUTOCOMMIT",
                echo_pool=True,
            )
            return engine
        except Exception as e:
            print(f"❌ Error al conectar a la base de datos: {e}")
            raise