"""
Created By: Juan David Ramirez
Script principal para generar mensajes de recordatorio
"""

import os
import sys

current_folder = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_folder)
sys.path.append(current_folder)

from generador_mensajes._cls_generador_mensajes import GeneradorMensajes


def main():
    """
    Ejecuta el proceso de generación de mensajes
    """
    
    # Ruta al archivo de configuración
    config_path = os.path.join(project_root, 'config', 'config_mensajes.json')
    
    # Crear instancia del generador
    generador = GeneradorMensajes(config_path=config_path)
    
    # Ejecutar proceso
    try:
        generador.ejecutar()
    except Exception as e:
        print(f"❌ Error en el proceso principal: {str(e)}")
        input("\nPresiona Enter para salir...")


if __name__ == '__main__':
    main()
