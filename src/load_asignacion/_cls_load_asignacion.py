"""
Created By Emerson Aguilar Cruz
"""
import os
from datetime import datetime
from conexiones_db._cls_sqlalchemy import MySQLConnector 
from read_data._cls_read_data import *
from load_data._cls_load_data import *
import json

import os
import json
from datetime import datetime

class LoadAsignacion:

    def __init__(self, config_path=None, ):
        
        self.fecha = datetime.now().strftime("%Y-%m-%d")
        
        self.current_folder = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.dirname(self.current_folder)
        self.project_home = os.path.dirname(self.project_root)

        self.config_path = config_path
        with open(self.config_path, 'r', encoding='utf-8') as f:
            self.config_asignacion = json.load(f)

        self.campanas_disponibles = [key for key in self.config_asignacion.keys() if key not in ['schema', 'table']]
        print("\n" + "="*40)
        print("🏥  CAMPAÑAS DISPONIBLES")
        print("="*40)
        for i, campaña in enumerate(self.campanas_disponibles, start=1):
            print(f"  [{i}] {campaña}" + "\n")

        while True:
            try:
                seleccion = int(input("Ingrese el número de la campaña que desea ejecutar: "))
                if 1 <= seleccion <= len(self.campanas_disponibles):
                    self.campana_seleccionada = self.campanas_disponibles[seleccion - 1]
                    break
                else:
                    print("Número inválido. Intente nuevamente.")
            except ValueError:
                print("Entrada no válida. Ingrese un número.")

        self.campana_config = self.config_asignacion[self.campana_seleccionada]

        self.start_path = os.path.join(self.project_home, 'data', 'asignacion', 'nueva', self.campana_config['nombre_asignacion'])
        self.end_path = os.path.join(self.project_home, 'data', 'asignacion', 'cargado', self.campana_config['nombre_asignacion'])
        os.makedirs(self.start_path, exist_ok=True)
        os.makedirs(self.end_path, exist_ok=True)
        
        self.schema = self.campana_config['schema']
        self.table = self.campana_config['table']

        self.engine = MySQLConnector().get_connection(database=self.schema)
        self.df = None
        self.loader = MySQLLoader(self.engine, self.schema, self.table)

        print("\n" + "─"*40)
        print(f"✓ Campaña seleccionada: {self.campana_seleccionada}")
        print(f"📂 Ruta origen: {self.start_path}")
        print(f"📁 Ruta destino: {self.end_path}")
        print("─"*40)

        
    def read_data(self):

        telefonos = self.campana_config['telefonos']

        if not hasattr(self, 'start_path') or not os.path.exists(self.start_path):
            raise ValueError(f"Ruta no válida: {getattr(self, 'start_path', 'No definida')}")

        files = [f for f in os.listdir(self.start_path) if os.path.isfile(os.path.join(self.start_path, f))]
        if not files:
            print(f"No se encontraron archivos en la ruta: {self.start_path}")
            return None

        try:
            reader = FileReader(start_path=self.start_path, end_path=self.end_path,)
            latest_file_path = reader.get_latest_file()

            if latest_file_path is None:
                print("No se pudo determinar el archivo más reciente")
                return None

            nombre_archivo = os.path.basename(latest_file_path)
            nombre_base = os.path.splitext(nombre_archivo)[0]
            
            hojas_disponibles, hoja_seleccionada = reader.sheet_names(latest_file_path)
            print("Hojas encontradas:", hojas_disponibles)
            print("Hoja seleccionada:", hoja_seleccionada)

            if hoja_seleccionada:
                self.df = reader.read_directory(latest_file_path, sheet_name=hoja_seleccionada)
            else:
                print("No se seleccionó una hoja válida. No se cargaron datos.")

            if self.df is None or self.df.empty:
                print("Error: No se pudo leer el archivo o está vacío")
                return None
            print(f"\n📊 Registros leídos: {len(self.df)}")

            self.df = self.df.rename(columns=self.campana_config['renombrar_columnas'])

            self.df['nombre_base'] = nombre_base
            self.df['hoja'] = hoja_seleccionada

            print(f"✓ Columnas procesadas: {len(self.df.columns)}")

            columnas_necesarias = self.campana_config['columnas_necesarias']

            columnas_existentes = [col for col in columnas_necesarias if col in self.df.columns]
            self.df = self.df[columnas_existentes]
            for col in self.df.columns:
                if self.df[col].dtype in ['float64', 'int64']:
                    self.df[col] = self.df[col].astype(object)
                self.df[col] = self.df[col].where(pd.notnull(self.df[col]), None)

 
            for col in self.df.select_dtypes(include='object').columns:
                self.df[col] = self.df[col].fillna('-')

            self.df = self.df.where(pd.notnull(self.df), None)
            
            def estandarizar_telefono(x):
                if pd.isna(x) or x == '-':
                    return '-'
                x = str(x).strip()
                x = '601' + x if len(x) == 7 and x.isdigit() else x
                return x.replace('.0', '')

            for col in telefonos:
                if col in self.df.columns:
                    self.df[col] = self.df[col].apply(estandarizar_telefono)
            
            # Dividir teléfonos múltiples (ejemplo: 3132896774-3143712382 o 3244122425 3045452344)
            print("\n📞 Procesando teléfonos múltiples...")
            for col in telefonos:
                if col in self.df.columns:
                    # Crear columna para segundo teléfono
                    col_secundario = f"{col}_2"
                    
                    # Función para dividir teléfonos
                    def dividir_telefonos(x):
                        if pd.isna(x) or x == '-':
                            return x, '-'
                        x = str(x).strip()
                        
                        # Detectar separador (guión o espacio)
                        if '-' in x:
                            partes = x.split('-')
                        elif ' ' in x:
                            # Filtrar espacios múltiples y dividir
                            partes = [p.strip() for p in x.split() if p.strip()]
                        else:
                            return x, '-'
                        
                        # Extraer teléfonos
                        tel1 = partes[0].strip() if len(partes) > 0 else '-'
                        tel2 = partes[1].strip() if len(partes) > 1 else '-'
                        
                        return tel1, tel2
                    
                    # Aplicar división
                    self.df[[col, col_secundario]] = self.df[col].apply(
                        lambda x: pd.Series(dividir_telefonos(x))
                    )
                    
                    # Contar cuántos teléfonos secundarios se encontraron
                    tel_secundarios = (self.df[col_secundario] != '-').sum()
                    if tel_secundarios > 0:
                        print(f"   ✓ {tel_secundarios} registros con teléfono secundario en {col}")

            columnas_fecha = self.campana_config['columnas_fecha']

            for col in columnas_fecha:
                if col in self.df.columns:
                    try:
                        self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
                    except Exception as e:
                        print(f"Error al convertir la columna {col} a fecha: {e}")
                        

            cols_duplicados = self.campana_config['cols_duplicados']

            if all(col in self.df.columns for col in cols_duplicados):
                filas_antes = len(self.df)

                df_duplicados = self.df[self.df.duplicated(subset=cols_duplicados, keep=False)]

            if not df_duplicados.empty:
                try:
                    self.loader.table_name = self.campana_config['table_duplicados']
                    self.loader.schema = self.schema
                    
                    self.loader.upsert_into_table(df_duplicados[cols_duplicados])

                    print(f"✓ Duplicados registrados: {len(df_duplicados)} registros")
                except Exception as e:
                    print(f"Error al insertar duplicados en SQL con upsert_into_table: {str(e)}")


                self.df.drop_duplicates(subset=cols_duplicados, inplace=True)
                print(f"🗑️  Duplicados eliminados: {filas_antes - len(self.df)}")
            else:
                print("Advertencia: Columnas para verificación de duplicados no encontradas")

            print(f"\n📈 Registros finales: {len(self.df)}")
            print("\n" + "="*40)
            print("✓ LECTURA COMPLETADA EXITOSAMENTE")
            print("="*40)

            for col in self.df.columns:
                if self.df[col].dtype in ['float64', 'int64']:
                    self.df[col] = self.df[col].astype(object)
                self.df[col] = self.df[col].where(pd.notnull(self.df[col]), None)

            if self.df.isnull().values.any():
                null_count = self.df.isnull().sum().sum()
                print(f"\n⚠ Advertencia: Se detectaron {null_count} valores nulos")
            return self.df

        except Exception as e:
            print(f"Error inesperado al leer datos: {str(e)}")
            return None
        

    def load_data(self):
        self.loader.table_name = self.campana_config['table']
        try:
            self.loader.upsert_into_table(self.df)
            print("\n" + "="*40)
            print("✅ CARGA COMPLETADA EXITOSAMENTE")
            print("="*40)
            print(f"📊 Tabla: {self.campana_config['table']}")
            print(f"📈 Registros insertados: {len(self.df)}")
            print("="*40 + "\n")
        except Exception as e:
            print(f"Error al insertar datos en la tabla de asignación: {str(e)}")

    

    def main(self):
        self.read_data()
        self.load_data()