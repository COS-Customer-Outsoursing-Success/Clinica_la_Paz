import pandas as pd
import numpy as np
from sqlalchemy import create_engine, MetaData, Table, text, Column, String, Integer, Numeric, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

class MySQLLoader:
    def __init__(self, engine, schema, table):
        """
        Inicializa el cargador de MySQL con la conexión ya establecida, esquema y tabla.
        """
        self.engine = engine
        self.schema = schema
        self.table_name = table
        self.metadata = MetaData(schema=self.schema)
        self.table = Table(self.table_name, self.metadata, autoload_with=self.engine)

        # Crea una sesión para manejar las transacciones
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def _sanitize_column_name(self, name):
        return name.replace(" ", "_").replace("-", "_").replace(".", "_")

    def close_session(self):
        """
        Cierra la sesión abierta.
        """
        if self.session:
            self.session.close()

    def truncate_table(self):
        """
        Trunca la tabla eliminando todos los datos existentes.
        """
        try:
            with self.engine.connect() as connection:
                truncate_query = f"TRUNCATE TABLE `{self.schema}`.`{self.table_name}`;"
                connection.execute(text(truncate_query))
                print(f"Tabla {self.schema}.{self.table_name} truncada correctamente.")
        except SQLAlchemyError as e:
            print(f"Error al truncar la tabla {self.table_name}: {e}")
        finally:
            self.session.commit()
            self.close_session()  

    def _get_current_columns(self):
        """
        Obtiene las columnas actuales de la tabla desde la base de datos.
        """
        with self.engine.connect() as connection:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(self.table_name, schema=self.schema)
            return [col['name'] for col in columns]

    def _add_missing_columns(self, missing_columns):
        """
        Agrega las columnas faltantes en la tabla mediante ALTER TABLE.
        """
        try:
            for column_name, column_type in missing_columns.items():
                alter_query = f"ALTER TABLE `{self.schema}`.`{self.table_name}` ADD COLUMN `{column_name}` {column_type};"
                with self.engine.connect() as connection:
                    connection.execute(text(alter_query))
                print(f"Columna {column_name} agregada correctamente.")
        except SQLAlchemyError as e:
            print(f"Error al agregar columna {column_name}: {e}")
        finally:
            self.session.commit()

    def _determine_column_type(self, value):
        """
        Determina el tipo de dato SQL adecuado para una nueva columna basada en el valor.
        """
        if isinstance(value, int):
            return "INT"
        elif isinstance(value, float):
            return "DECIMAL(18, 4)"
        elif isinstance(value, pd.Timestamp):
            return "DATETIME"
        elif isinstance(value, bool):
            return "TINYINT(1)"
        elif isinstance(value, str):
            max_len = max(255, len(value))  # Puedes ajustar esto según tus reglas
            return f"VARCHAR({min(max_len, 1000)})"
        else:
            return "VARCHAR(255)"


    def replace_into_table(self, data):
        """
        Realiza un REPLACE INTO basado en la clave primaria.
        'data' debe ser un DataFrame que representa los valores a insertar.
        """
        if data.empty:
            print("No hay datos para insertar.")
            return

        try:
            # Obtener las columnas actuales de la tabla
            current_columns = self._get_current_columns()
            print(f"\n📋 Columnas en BD: {len(current_columns)}")

            # Determinar si hay columnas nuevas que no están en la tabla
            new_columns = {}
            for key in data.columns:
                if key not in current_columns:
                    non_null_values = data[key].dropna()
                    if not non_null_values.empty:
                        sample_value = non_null_values.iloc[0]
                        column_type = self._determine_column_type(sample_value)
                        new_columns[key] = column_type
                    else:
                        print(f"No se encontró valor no nulo para la columna '{key}'. Se omitirá la adición de esta columna.")

            if new_columns:
                print(f"➕ Agregando {len(new_columns)} columnas nuevas...")
                self._add_missing_columns(new_columns)
                self.metadata.clear()
                self.table = Table(self.table_name, self.metadata, autoload_with=self.engine)

            # Preparar la consulta REPLACE INTO
            columns = ', '.join([f"`{key}`" if key.isdigit() else key for key in data.columns])
            placeholders = ', '.join([f":{key}" if key.isdigit() else f":{key}" for key in data.columns])

            replace_query = f"""
            REPLACE INTO `{self.schema}`.`{self.table_name}` ({columns})
            VALUES ({placeholders})
            """

            # Ejecutar REPLACE para cada fila
            with self.engine.connect() as connection:
                with connection.begin():
                    for _, row in data.iterrows():
                        row_dict = row.to_dict()
                        connection.execute(text(replace_query), row_dict)

            print(f"Datos reemplazados en la tabla {self.schema}.{self.table_name} correctamente.")

        except SQLAlchemyError as e:
            print(f"Error al hacer REPLACE INTO en la tabla {self.table_name}: {e}")
        finally:
            self.session.commit()
            self.close_session()  # Cerrar la sesión

    def _chunk_dataframe(self, df, size=10000):
        for start in range(0, len(df), size):
            yield df.iloc[start:start + size]

    def upsert_into_table(self, data, table_name=None, schema=None):
        """
        Realiza un INSERT INTO basado en la clave primaria.
        Si existe un conflicto en la clave primaria, actualiza solo las columnas proporcionadas en 'data'.
        'data' debe ser un DataFrame que representa los valores a insertar.
        """
        if data.empty:
            print("No hay datos para insertar.")
            return

        if table_name:
            self.table_name = table_name
        if schema:
            self.schema = schema
        try:
            current_columns = self._get_current_columns()
            print(f"Columnas actuales en la tabla: {current_columns}")

            new_columns = {}
            for key in data.columns:
                if key not in current_columns:
                    non_null_values = data[key].dropna()
                    if not non_null_values.empty:
                        sample_value = non_null_values.iloc[0]
                        column_type = self._determine_column_type(sample_value)
                        new_columns[key] = column_type
                    else:
                        print(f"No se encontró valor no nulo para la columna '{key}'. Se omitirá la adición de esta columna.")

            if new_columns:
                print(f"Agregando nuevas columnas: {new_columns}")
                self._add_missing_columns(new_columns)
                self.metadata.clear()
                self.table = Table(self.table_name, self.metadata, autoload_with=self.engine)

            columns = ', '.join([f"`{key}`" for key in data.columns])
            placeholders = ', '.join([f":{key}" for key in data.columns])
            update_columns = ', '.join([f"`{key}` = VALUES(`{key}`)" for key in data.columns])

            insert_query = f"""
            INSERT INTO `{self.schema}`.`{self.table_name}` ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_columns}
            """

            with self.engine.connect() as connection:
                with connection.begin():
                    for _, row in data.iterrows():
                        row_dict = row.to_dict()
                        connection.execute(text(insert_query), row_dict)

            print(f"✓ Datos procesados: {len(data)} registros")

        except SQLAlchemyError as e:
            print(f"Error al hacer INSERT INTO en la tabla {self.table_name}: {e}")
        finally:
            self.session.commit()
            self.close_session()  # Cerrar la sesión

    def delete_all_and_insert(self, data):
        """
        Elimina todos los registros de la tabla y luego inserta los nuevos datos del DataFrame.
        Similar a un TRUNCATE, pero mantiene estructura e índices sin reiniciar autoincrementos.
        Además, limpia valores NaN/NaT/pd.NA para evitar errores de inserción.
        """
        if data.empty:
            print("No hay datos para insertar.")
            return


        data = data.replace({pd.NA: None, np.nan: None})

        data = data.applymap(lambda x: str(x).strip() if isinstance(x, str) else x)

        try:
            current_columns = self._get_current_columns()
            new_columns = {}

            # Verifica si hay columnas nuevas en el DataFrame que no existen en la tabla
            for key in data.columns:
                if key not in current_columns:
                    non_null_values = data[key].dropna()
                    if not non_null_values.empty:
                        sample_value = non_null_values.iloc[0]
                        column_type = self._determine_column_type(sample_value)
                        new_columns[key] = column_type

            # Si hay columnas nuevas, las agrega
            if new_columns:
                print(f"Agregando nuevas columnas: {new_columns}")
                self._add_missing_columns(new_columns)
                self.metadata.clear()
                self.table = Table(self.table_name, self.metadata, autoload_with=self.engine)

            delete_query = f"DELETE FROM `{self.schema}`.`{self.table_name}`"

            columns = ', '.join([f"`{key}`" for key in data.columns])
            placeholders = ', '.join([f":{key}" for key in data.columns])
            insert_query = f"INSERT INTO `{self.schema}`.`{self.table_name}` ({columns}) VALUES ({placeholders})"

            with self.engine.connect() as connection:
                with connection.begin():
                    connection.execute(text(delete_query))
                    for _, row in data.iterrows():
                        connection.execute(text(insert_query), row.to_dict())

            print(f"Todos los datos de {self.schema}.{self.table_name} fueron eliminados e insertados correctamente.")

        except SQLAlchemyError as e:
            print(f"Error en delete_all_and_insert: {e}")
        finally:
            self.session.commit()
            self.close_session()
