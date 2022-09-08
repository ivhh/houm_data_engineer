# Desafío técnico para Ingeniero de Datos

Se sólicito poder responder ciertas preguntas teniendo en consideración
un dataset (incluído en la carpeta datasets de este proyecto) y una API
de clima ([link](https://www.visualcrossing.com/)).

## Preguntas del desafio

- ¿Cuántas visitas se realizaron en total?
- ¿Cuál es el promedio de propiedades por propietario?
- ¿Cuál era la temperatura promedio de todas las visitas que realizó en  la propiedad del propietario con ID 2?
- ¿Cuál es la temperatura promedio de las visitas para los días con lluvia?
- ¿Cuál es la temperatura promedio para las visitas realizadas en la localidad de Suba?

## Consideraciones

Para resolver el problema se tomo varias consideraciones

1- Se utilizó una librería de autoría propia para el llamado REST de la API. (Le daría más robustez en un caso real)

2- Dada las restricciones de la API de 1000 llamados, se considera que el volumen de datos al correr el programa es el que se entregó y no uno 
considerablemente mayor

3- El programa corresponde a un script sencillo

4- Las variables importantes como API key deben ser protegidas y por tanto no están en el repositorio.

## Ejecución del programa

Para ejecutar el programa se deben seguir los siguientes pasos preeliminares:

- Tener instalado Python versión 3.7 o superior
- Instalar venv con las librerías dispuestas en requirements.txt o en su defecto correrlo instalarlas en el ambiente local
- Colocar valor de API key en la variable de entorno WEATHER_API_KEY

Una vez listo esto, en la carpeta en que se clonó este programa, utilizar el comando:

```bash
python main.py
```

## Dudas o consultas

Cualquier duda, consulta o feedback de este desafío técnico, pueden comunicarse conmigo a ivan.huerta.h@gmail.com