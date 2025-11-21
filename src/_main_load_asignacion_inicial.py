"""""
Created By Emerson Aguilar Cruz
"""""

import os
import sys

current_folder = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_folder)
sys.path.append(current_folder)

from load_asignacion._cls_load_asignacion import LoadAsignacion
    
def main():

    config_path = os.path.join(project_root, 'config', 'config_asignacion_inicial.json')
    
    # -- Inicializador de clases -- 
    loader_asignacion = LoadAsignacion(
        config_path = config_path

    )

    try:
        loader_asignacion.main()
    except Exception as e:
        print(f"Error en el proceso principal: {str(e)}")

if __name__ == '__main__':

    main()