# Web scrapper para extraer datos de la web de IGME

## Instrucciones
1. Copiar la carpeta `test_de_prueba` con otro nombre
2. Modificar el archivo `config.yaml` asignando las variables a extraer
   1. Apuntar a los archivos Excel de búsqueda usando una lista `excel_files: ['archivo1.xlsx', 'archivo2.xlsx']`
   2. Modificar variables objectivo `target_variables`. Las opciones son: `['Medidas de piezometría', 'Litologías', 'Análisis químicos']`)
   
3. Ejecutar el script `run.py` desde PyCHARM