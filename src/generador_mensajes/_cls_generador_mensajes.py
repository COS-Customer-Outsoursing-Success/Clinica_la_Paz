"""
Created By: Juan Ramirez
Clase para generar mensajes personalizados de recordatorio de citas
"""

import os
import json
import pandas as pd
from datetime import datetime
from conexiones_db._cls_sqlalchemy import MySQLConnector


class GeneradorMensajes:    
    def __init__(self, config_path):
        """
        Inicializa el generador de mensajes
        
        Args:
            config_path (str): Ruta al archivo de configuración JSON
        """
        self.config_path = config_path
        
        # Cargar configuración
        with open(self.config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        self.config = config['mensajes_clinica_la_paz']
        
        # Rutas del proyecto
        self.current_folder = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.dirname(self.current_folder)
        self.project_home = os.path.dirname(self.project_root)
        
        # Crear carpetas de salida
        self.output_path = os.path.join(self.project_home, 'data', 'mensajes')
        os.makedirs(self.output_path, exist_ok=True)
        
        # Conexión a la BD
        self.schema = self.config['schema']
        self.engine = MySQLConnector().get_connection(database=self.schema)
        
        # DataFrame de resultados
        self.df_citas = None
        
        print("\n" + "="*40)
        print("💬  GENERADOR DE MENSAJES - CLÍNICA LA PAZ")
        print("="*40)
        print(f"📁 Carpeta salida: {self.output_path}")
        print("="*40 + "\n")
    
    
    def ejecutar_query(self):
        """
        Ejecuta el query SQL para obtener las citas del día
        """
        print("🔍 Consultando citas del día...")
        
        # Leer el archivo SQL
        sql_file = os.path.join(self.project_home, 'sql', self.config['query_file'])
        
        with open(sql_file, 'r', encoding='utf-8') as f:
            query = f.read()
        
        # Ejecutar query
        self.df_citas = pd.read_sql(query, self.engine)
        
        print(f"✓ Citas encontradas: {len(self.df_citas)}")
        
        if len(self.df_citas) == 0:
            print("⚠ No hay citas programadas para hoy")
            return False
        
        return True
    
    
    def formatear_fecha_hora(self, fecha_hora):
        """
        Convierte datetime a formato legible en español
        """
        # Convertir a datetime si es string
        if isinstance(fecha_hora, str):
            fecha_hora = pd.to_datetime(fecha_hora)
        
        # Nombre del día en inglés
        dia_semana_en = fecha_hora.strftime("%A")
        dia_semana_es = self.config['nombres_dias'][dia_semana_en]
        
        # Día del mes
        dia = fecha_hora.day
        
        # Mes en español
        mes_num = fecha_hora.strftime("%m")
        mes_es = self.config['nombres_meses'][mes_num]
        
        # Año
        anio = fecha_hora.year
        
        # Formato fecha: "Jueves 21 de Noviembre de 2025"
        fecha_formateada = f"{dia_semana_es} {dia} de {mes_es} de {anio}"
        
        # Formato hora: "1:30 PM"
        hora = fecha_hora.hour
        minutos = fecha_hora.minute
        
        # Determinar AM/PM
        if hora >= 12:
            am_pm = "PM"
            if hora > 12:
                hora = hora - 12
        else:
            am_pm = "AM"
            if hora == 0:
                hora = 12
        
        hora_formateada = f"{hora}:{minutos:02d} {am_pm}"
        
        return fecha_formateada, hora_formateada
    
    
    def identificar_modalidad(self, consultorio, centro):
        """
        Identifica la modalidad de la cita (presencial o virtual) y la sede
        """
        consultorio = str(consultorio).lower()
        centro = str(centro).lower()
        
        # Verificar si es virtual
        palabra_virtual = self.config['reglas_identificacion']['modalidad_virtual']['palabra_clave']
        if palabra_virtual in consultorio:
            return "virtual"
        
        # Si no es virtual, es presencial - determinar sede
        palabra_uniminuto = self.config['reglas_identificacion']['sede_uniminuto']['palabra_clave']
        if palabra_uniminuto in centro:
            return "presencial_uniminuto"
        else:
            return "presencial_principal"
    
    
    def generar_mensaje(self, row):
        """
        Genera el mensaje personalizado para una cita
        """
        # Extraer datos
        nombre = str(row['pacientenombre']).strip()
        especialidad = str(row['especialinom']).strip()
        profesional = str(row['mediconom']).strip()
        fecha_hora = row['fechacitaini']
        consultorio = row['consultorionom']
        centro = row['centronom']
        
        # Formatear fecha y hora
        fecha, hora = self.formatear_fecha_hora(fecha_hora)
        
        # Identificar modalidad
        modalidad = self.identificar_modalidad(consultorio, centro)
        
        # Obtener plantilla
        plantilla = self.config['plantillas'][modalidad]
        
        # Reemplazar variables
        mensaje = plantilla.format(
            nombre=nombre,
            especialidad=especialidad,
            profesional=profesional,
            fecha=fecha,
            hora=hora
        )
        
        return mensaje
    
    
    def procesar_mensajes(self):
        """
        Procesa todas las citas y genera los mensajes
        """
        print("\n📝 Generando mensajes personalizados...")
        
        # Generar mensajes para cada fila
        self.df_citas['mensaje'] = self.df_citas.apply(self.generar_mensaje, axis=1)
        
        print(f"✓ Mensajes generados: {len(self.df_citas)}")
        
        # Crear DataFrame de salida con solo teléfono y mensaje
        df_output = self.df_citas[['phone', 'mensaje']].copy()
        df_output.columns = ['telefono', 'mensaje']
        
        return df_output
    
    
    def exportar_excel(self, df_output):
        """
        Exporta el DataFrame a Exce
        """
        print("\n💾 Exportando archivo Excel...")
        
        # Nombre del archivo con fecha y hora
        fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_archivo = f"mensajes_clinica_la_paz_{fecha_actual}.xlsx"
        ruta_completa = os.path.join(self.output_path, nombre_archivo)
        
        # Exportar
        df_output.to_excel(ruta_completa, index=False, engine='openpyxl')
        
        print(f"✓ Archivo exportado: {nombre_archivo}")
        print(f"📂 Ubicación: {ruta_completa}")
        
        return ruta_completa
    
    
    def generar_resumen(self, df_output):
        """
        Genera un resumen del proceso
        """
        print("\n" + "="*40)
        print("📊 RESUMEN DEL PROCESO")
        print("="*40)
        print(f"📱 Total de mensajes generados: {len(df_output)}")
        print(f"📅 Fecha: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        print("="*40 + "\n")
    
    
    def ejecutar(self):
        """
        Ejecuta el proceso completo
        """
        try:
            # 1. Ejecutar query
            if not self.ejecutar_query():
                return
            
            # 2. Generar mensajes
            df_output = self.procesar_mensajes()
            
            # 3. Exportar Excel
            archivo_generado = self.exportar_excel(df_output)
            
            # 4. Resumen
            self.generar_resumen(df_output)
            
            print("✅ Proceso completado exitosamente\n")
            
        except Exception as e:
            print(f"\n❌ Error en el proceso: {str(e)}\n")
            raise
