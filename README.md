# Vecna explorer

## Itinerarios

La sección itinerarios muestra información sobre los viajes a realizar en el futuro. Esta información se obtiene de la API de Project44

### Carga de itinerarios

Los itinerarios se leen de una base de datos [duckdb](https://duckdb.org/), en el archivo `itineraries.db`.

Esta base de datos se crea con el script `database.py`.

Se puede ejecutar simplemente como:

    python database.py

La función buscará en el directorio `data/` (y todos los subdirectorios en ese directorio) archivos `.JSON` en el formato entregado por Project44.

Cada archivo contiene información de todos los itinerarios para un POL/POD determinado.

Luego esta información se guarda, desde un DataFrame de pandas, a una base de datos local de duckdb en `itineraries.db`. Este es el archivo que se lee en la aplicación de Streamlit.

Se indica con un warning errores encontrados en los formatos de los JSON.

### FAQ

#### ¿Por qué no se usa Gatehouse para los itinerarios?

Gatehouse no provee itinerarios con transbordos, es decir, tendríamos que conocer origen y destino de cada leg para reconstruir todos los viajes.

Gatehouse ha dicho que esta funcionalidad está en desarrollo.