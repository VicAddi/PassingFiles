TAG_EXCEL_SHEET_DATAFRAME_PROMPT = """Eres un experto en etiquetado de conjuntos de datos cuya INFORMACIÓN representa datos de asegurados o siniestros de una aseguradora.
 
# CONJUNTO DE DATOS PARA ETIQUETAR
 
${tabular_data_markdown_str}
 
Por favor, presta mucha atención a las siguientes definiciones de etiquetas:
 
# ETIQUETA: CENSUS
 
La etiqueta CENSUS se define como la etiqueta que debe asignarse al conjunto de datos cuya información enuncia los siguientes aspectos de un conjunto de personas:
 
## ASPECTOS QUE DEFINEN AL CONJUNTO DE DATOS CUYA ETIQUETA ES "CENSUS":
 
* NOMBRE o NOMBRES
* APELLIDO PATERNO
* APELLIDO MATERNO
* EDAD o FECHA DE NACIMIENTO O FEC. DE NAC.
* SEXO o GÉNERO
* SUBGRUPO O SUBGRUPO SLIP
* NOMBRE DE GRUPO
* RELACION
 
 
Para que una tabla sea etiquetada como 'CENSUS', dicha tabla debe contar con la siguiente información, esta informacion puede venir en los formatos que aparecen a continuacion:
 
["NOMBRE o NOMBRES o NOMBRE COMPLETO", "EDAD o FECHA DE NACIMIENTO", "SEXO o GÉNERO" ]
["SUBGRUPO O SUBGRUPO SLIP", "NOMBRE DE GRUPO", "RELACION", "GENERO", "FEC. DE NAC."]
 
Cuando encuentres tablas que son alusivas a CENSUS pero que NO cuentan con la información entonces la decisión es ambigua y por lo tanto asignarás 'SIN ETIQUETA'. Sin embargo recuerda que existen esos dos formatos o puede que haya mas
 
## CAMPOS QUE SE ESPERA EXTRAER DEL CONJUNTO DE DATOS ETIQUETADO COMO CENSUS:
 
El objetivo de la etiqueta CENSUS es identificar el conjunto de datos cuya información tiene potencial para la extracción de los siguientes campos:
 
["Póliza", "Subgrupo", "Numero SubGrupo", "Certificado", "Parentesco", "Género", "Fecha de nacimiento"]
 
La etiqueta CENSUS, se concentra únicamente en información personal, lo que define la unidad mínimia de su arreglo tabular como "UNA PERSONA".
Es decir, cada uno de los renglones de una tabla cuya etiqueta es CENSUS, representa a una sola persona, y las columnas son la información personal asociada a esta.
 
# ETIQUETA: SINIESTRALIDAD
 
La etiqueta SINIESTRALIDAD se define como la etiqueta que debe asignarse al conjunto de datos cuya información enlista los siniestros reportados por una aseguradora a lo largo de un periodo de tiempo.
La etiqueta SINIESTRALIDAD siempre representa los datos de un histórico de SINIESTROS.
 
## ASPECTOS QUE DEFINEN AL CONJUNTO DE DATOS CUYA ETIQUETA ES "SINIESTRALIDAD":
 
Los aspectos que definen al conjunto de datos cuya etiqueta es "SINIESTRALIDAD" son los siguientes:
 
* IDENTIDICADOR DE SINIESTRO
* PADECIMIENTO o INCIDENCIA MÉDICA
* MONTO RECLAMADO o MONTO PAGADO
* DEDUCIBLE
* COASEGURO
* IVA
* TIPO DE PAGO
* FECHA DE PAGO
 
Para que una tabla sea etiquetada como "SINIESTRALIDAD", dicha tabla se caracteriza por presencia de los siguientes campos:
 
["IDENTIFICADOR DE SINIESTRO", "MONTO PAGADO", "TIPO DE PAGO", "FECHA DE PAGO", "PADECIMIENTO"]

La presencia de estos campos es flexible para asignar la etiqueta de SINIESTRALIDAD. La decisión de etiquetado debe basarse en un análisis detallado de la información contenida en la tabla.

## CAMPOS QUE SE ESPERA EXTRAER DEL CONJUNTO DE DATOS ETIQUETADO COMO SINIESTRALIDAD:
 
El objetivo de la etiqueta SINIESTRALIDAD es identificar el conjunto de datos cuya información tiene potencial para la extracción de los siguientes campos:
 
["ID de SINIESTRO", "Padecimiento", "Fecha de Pago", "Tipo de Pago", "Monto Pagado Reportado"]
 
La etiqueta CENSUS, se concentra únicamente en información sobre siniestros, lo que define la unidad mínimia de su arreglo tabular como "UN SINIESTRO".
Es decir, cada uno de los renglones de una tabla cuya etiqueta es SINIESTRALIDAD, representa un solo siniestro y las columnas son la información o detalles del siniestros, siempre enfocados en el detalle moentario y de padecimiento cubierto.
 
# TU TAREA COMO experto en etiquetado de conjuntos de datos
 
Por favor, sigue paso a paso las siguientes instrucciones
 
1. Analiza con mucho cuidado la información proporcionada en la sección "CONJUNTO DE DATOS PARA ETIQUETAR".
2. Presta atención a los más mínimos detalles.
3. Etiqueta el conjunto de datos en función de las etiquetas definidas. CENSUS o SINIESTRALIDAD.
4. Establece un análisis/razonamiento que enliste paso a paso los puntos clave que fundamentan la decisión de etiquetado que tomaste.
5. Cuando la decisión de etiquetado es ambigua entonces explica que no es posible llegar a una conclusión y que por lo tanto la etiqueta será 'SIN ETIQUETA'.
6. Genera tu respuesta formato JSON String, dicho JSON debe contener únicamente las llaves:
    a. 'reasoning_process': Esta llave contiene el proceso de análisis/razonamiento que fundamentan la decisión de etiquetado que tomaste.
    b. 'dataset_tag': Esta llave contiene la etiqueta que decidiste asignarle al conjunto de datos. Cuando no pudiste asignar una etiqueta, entonces pondrás 'SIN ETIQUETA'.
7. Para generar la respuesta formato JSON String sigue estricta y únicamente la definición del JSON schema siguiente:
{
    "reasoning_process": {
            "type": "string",
            "description": "Representa el proceso de análisis/razonamiento que fundamentan la decisión de etiquetado que tomaste."
        },
    "dataset_tag": {
            "type": "string",
            "description": "Representa la etiqueta que decidiste asignarle al conjunto de datos. Cuando no pudiste asignar una etiqueta, entonces pondrás 'SIN ETIQUETA'",
        }
}
8. Es sumamente importante que te enfoques en generar un JSON String válido, pues la idea es usar lenguaje Python y la función json.loads para obtener un diccionario de python a partir de tu respuesta.
"""
 
 
FIND_TEMPLATE_FIELD_ON_DATAFRAME_PROMPT = """Eres un experto encontrando campos en un tabla.
 
# MUESTRA DEL CONJUNTO DE DATOS DONDE DEBES BUSCAR
 
${tabular_data_markdown_str}
 
Por favor, presta mucha atención al siguiente título y descripción del campo que tienes que encontrar:
 
## Título de campo
 
${field_title}
 
## Descripción de campo
 
${field_description}
 
Tu tarea es encontrar el título de la única columna dentro de la 'MUESTRA DEL CONJUNTO DE DATOS DONDE DEBES BUSCAR' que coincida con la descripción dada, para ello por favor apegate a las siguiente instrucciones:
 
1. Analiza con mucho detalle las columnas y los datos de la 'MUESTRA DEL CONJUNTO DE DATOS DONDE DEBES BUSCAR'.
2. Encuentra similitudes entre las columnas y el campo que tienes que encontrar apoyándote de la descripción del campo.
3. Establece un proceso de razonamiento paso a paso en el que utilizas la descripción del campo para encontrar la columna que coincide, para ello.
4. Todas las decisiones que tomes respecto al campo encontrado deben justificarse a partir de la descripción del campo para encontrar la columna que coincide.
5. Cuando la 'MUESTRA DEL CONJUNTO DE DATOS DONDE DEBES BUSCAR' no contiene evidencia suficiente para encontrar el campo entonces la decisión es ambigua y por lo tanto el campo 'SIN COLUMNA ENCONTRADA'.
6. Genera tu respuesta formato JSON String, dicho JSON debe contener únicamente las llaves:
    a. 'reasoning_process': Esta llave contiene el proceso de análisis/razonamiento que fundamenta la decisión que tomaste respecto a la selección de la columna que coincide con la descripción del campo.
    b. 'matched_column_name': Esta llave contiene el nombre de la columna de la 'MUESTRA DEL CONJUNTO DE DATOS DONDE DEBES BUSCAR' que has seleccionado. Recuerda, Cuando la 'MUESTRA DEL CONJUNTO DE DATOS DONDE DEBES BUSCAR' no contiene evidencia suficiente para encontrar el campo entonces la decisión es ambigua y por lo tanto el campo 'SIN COLUMNA ENCONTRADA'.
7. Para generar la respuesta formato JSON String sigue estricta y únicamente la definición del JSON schema siguiente:
{
    "reasoning_process": {
            "type": "string",
            "description": "Representa el proceso de análisis/razonamiento que fundamenta la decisión que tomaste respecto a la selección de la columna que coincide con la descripción del campo."
        },
    "matched_column_name": {
            "type": "string",
            "description": "Representa el nombre de la columna de la 'MUESTRA DEL CONJUNTO DE DATOS DONDE DEBES BUSCAR' que has seleccionado. Recuerda, Cuando la 'MUESTRA DEL CONJUNTO DE DATOS DONDE DEBES BUSCAR' no contiene evidencia suficiente para encontrar el campo entonces la decisión es ambigua y por lo tanto el campo 'SIN COLUMNA ENCONTRADA'",
        }
}
8. Es sumamente importante que te enfoques en generar un JSON String válido, pues la idea es usar lenguaje Python y la función json.loads para obtener un diccionario de python a partir de tu respuesta.
"""
 
MAP_TIPO_PAGO_COLUMN_VALUES_PROMPT = """Eres un experto homologando datos.
 
# Datos para homologar
 
${pago_directo_column_unique_values}
 
Por favor sigue las instrucciones:
 
1. Analiza con muchísimo detalle los valores que se te muestran en la sección 'Datos para homologar'.
2. Agrupa cada uno de los datos en tres grupos titulados "Pago directo", "Reembolso", "Desconocido".
3. El grupo "Pago directo" debe contener todos los valores que hacen referencia a cualquier tipo de pago que haya sido directo.
4. El grupo "Reembolso" debe contener todos los valores que hacen referencia a cualquier tipo de pago que haya sido reembolso.
5. El grupo "Desconocido" debe contener todos los valores que no tienen evidencia suficiente para agruparlos en el grupo 'Pago directo' o 'Reembolso'.
6. Genera tu respuesta formato JSON String, dicho JSON debe contener únicamente las llaves:
    a. 'reasoning_process': Esta llave contiene el proceso de análisis/razonamiento que fundamenta la decisión que tomaste respecto a la selección de la columna que coincide con la descripción del campo.
    b. 'grouping_dict': Esta llave contiene un JSON dónde cada llave representa el valor mostrado en la sección 'Datos para homologar' y cada valor asociado a dicha llave corresponde con el titulo del grupo asignado.
7. Para generar la respuesta formato JSON String sigue estricta y únicamente la definición del esquema de JSON siguiente:
{
    "reasoning_process": {
            "type": "string",
            "description": "Representa el proceso de análisis/razonamiento que fundamenta la decisión que tomaste respecto a la agrupación de los datos en la sección 'Datos para homologar'"
        },
    "grouping_dict": {
            "type": "object",
            "description": "Esta llave contiene un JSON dónde cada llave representa el valor mostrado en la sección 'Datos para homologar' y cada valor asociado a dicha llave corresponde con el titulo del grupo asignado.",
        }
}
8. Es sumamente importante que te enfoques en generar un JSON String válido, pues la idea es usar lenguaje Python y la función json.loads para obtener un diccionario de python a partir de tu respuesta.
 
A continuación te muestro un ejemplo de la actividad:
 
Supongamos que los datos para la homolagar son los siguientes:
 
['PAGO DIRECTO MÉDICO', 'PAGO DIRECTO HOSPITAL', 'REEMBOLSO', 'RH', 'UN VALOR AMBIGUO']
 
Entonces el resultado debe ser:
 
{
    "reasoning_process": "Analicé e interprete lo valores de la sección Datos para homologar y he construído el diccionario que homologa los datos.",
    "grouping_dict": {
        "PAGO DIRECTO MÉDICO": "Pago directo",
        "PAGO DIRECTO HOSPITAL": "Pago directo",
        "REEMBOLSO": "Reembolso",
        "RH": "Desconocido",
        "UN VALOR AMBIGUO": "Desconocido",
    }
}
Destaca el hecho de que los grupos que deben tener muchísima certeza de ser homologados son los grupos "Pago directo" y "Reembolso".
Por favor, enfócate en generar un JSON String válido, pues la idea es usar lenguaje Python y la función json.loads para obtener un diccionario de python a partir de tu respuesta.
"""
 
 
# NOTE: Gracias a los templates tenemos la descripción de los campos que deben llenarse usando la data de los archivos EXCEL
 
# 1. Necesitamos describir los campos del template de la forma más clara y específica posible
 
# CENSUS
CENSUS_FIELDS_DESCRIPTIONS_TO_FIND_DICT = {
    "Póliza": "Este es el campo que identifica la póliza asociada a la persona o empresa. Recordando que la tabla de census tiene como unidad muestral una persona o empresa, es importante recordar que en algunos casos no se contiene el número de póliza en formato alfanumerico, en ese caso se debe mantener el campo vacio",
    "Subgrupo": "Este campo representa el subgrupo al que pertenece la persona. El campo suele nombrarse SubGrupo, COMITE, FILIAL o abreviaciones asociadas a estos posibles titulos.",
    "Numero Sgrp": "Representa el número asociado al sub grupo, este campo siempre se nombra Numero de Sub Grupo SubGrupo, COMITE, FILIAL o abreviaciones asociadas a estos posibles titulos.",
    "Certificado": "Este es un campo alfanumerico y en la mayoría de los casos el título de la columna es literalmente 'Certificado'.",
    "Parentesco": "Este campo enuncia los valores de Parentesco del asegurado, recordando que la tabla de census es una tabla cuya unidad muestral es una persona, entonces el parentesco de esta persona está relacionada con las etiqueta de Cónyuge, Empleado o Hijo.",
    "Género": "Representa el género de la persona, usualmente puede encontrarse con nombres como 'Sexo' o 'Género' y los valores de este campo son los que se relacionan con el género de una persona Masculino, Femenino, Hombre, Mujer.",
    "Fecha de nacimiento": "Este campo representa la fecha de nacimiento de la persona, no debe confundirse con ninguna otra fecha, es estrictamente la fecha de nacimiento.",
}
 
# NOTE: Para CENSUS, los campos: ["Pagos", "%descuento aplicado"] son campos calculados, así como lo es el campo "ASEGURADORA"
# para este último decidimos dejarlo en el flujo de identificación y mapeo de columnas, de momento, si razón aparente
 
# SINIESTRALIDAD
SINIESTRALIDAD_FIELDS_DESCRIPTIONS_TO_FIND_DICT = {
    "POLIZA": "Este es el campo que identifica la póliza asociada al siniestro a través de un identificador. Recordando que la tabla de census tiene como unidad muestral un siniestro.",
    "ASEGURADORA": "Este campo representa el nombre de la aseguradora que atendió el siniestro, usualmente los valores que llenan este campo representan el nombre comercial de la aseguradora, es decir, AXA México, Metlife México, entre otras.",
    "SINIESTRO": "Este es un campo alfanumerico que representa el ID del Siniestro. Es estrictamente un campo identificador asociado al Siniestro, recordando que la tabla de SINIESTRALIDAD tiedne unidad muestral 'un siniestro'.",
    "Padecimiento": "Este campo representa la descripción del padecimiento reportado en el siniestro usualmente se nombra Padecimiento, Diagnoóstico o Descripción de Padecimiento.",
    "Fecha de Pago": "Este campo representa la fecha de pago del siniestro",
    "Tipo de Pago": "Representa la forma en la que se pagó el siniestro, algunos de los valores que puede tomar este campo son PAGO DIRECTO HOSPITAL, PAGO DIRECTO MÉDICO, REEMBOLSO, es decir, la forma en la que se pagó el siniestro.",
    "Monto Pagado Reportado": "Este campo representa el monto monetario Pagado, no se debe confundir por ningún motivo con otros montos monetarios alusivos al pago y que usualmente refieren a 'MONTO RECLAMADO', 'DEDUCIBLE', 'COASEGURO', 'IVA'. El titulo de este campo tiende a ser 'Pagado' o 'PAGADO', recuerda no confundir con ningún otro monto monetario.",
    "IVA Pagado": "Este campo representa el monto monetario asociado a lo que se conoce en México como IVA, Impuesto al Valor Agregado. Es muy importante identificar cuando este se encuntra presente en los datos, pues en conjunto con el campo Monto Pagado Reportado serán de utilidad para los reportes financieros.",
}
 
# NOTE: Para SINIESTRALIDAD, los campos: ["Pagos", "%descuento aplicado"] son campos calculados, así como lo es el campo "ASEGURADORA"
# para este último decidimos dejarlo en el flujo de identificación y mapeo de columnas.
 
 
MAP_PARTNERSHIP_COLUMN_VALUES_PROMPT = """Eres un experto homologando datos.
 
# Datos para homologar
 
${parentesco_column_unique_values}
 
Por favor sigue las instrucciones:
 
1. Analiza con muchísimo detalle los valores que se te muestran en la sección 'Datos para homologar'.
2. Agrupa cada uno de los datos en cuatro grupos titulados "Cónyuge", "Hijo", "Empleado", "Desconocido".
3. El grupo "Cónyuge" debe contener todos los valores que hacen referencia a cónyuge, esposa, esposo y todas las variantes de sustantivos para utilizadas referirse al concubinato entre dos personas.
4. El grupo "Hijo" debe contener todos los valores que hacen referencia a Hijo, Hija, Dependiente y todas las variantes de sustantivos utilizadas para referirse al hijo de una persona.
5. El grupo "Empleado" debe contener todos los valores que hacen referencia a empleado, empleada y a todas las variantes de sustantivos utilizadas para referirse al empleado de una empres.
6. Caso especial: Toda referencia al sustantitvo "TITULAR" debe ser mapeada al grupo "Empleado".
7. El grupo "Desconocido" debe contener todos los valores que no tienen evidencia suficiente para agruparlos en algunos de los grupos "Cónyuge", "Hijo", "Empleado".
8. Genera tu respuesta formato JSON String, dicho JSON debe contener únicamente las llaves:
    a. 'reasoning_process': Esta llave contiene el proceso de análisis/razonamiento que fundamenta la decisión que tomaste respecto a la selección de la columna que coincide con la descripción del campo.
    b. 'grouping_dict': Esta llave contiene un JSON dónde cada llave representa el valor mostrado en la sección 'Datos para homologar' y cada valor asociado a dicha llave corresponde con el titulo del grupo asignado.
9. Para generar la respuesta en formato JSON String sigue estricta y únicamente la definición del esquema de JSON siguiente:
{
    "reasoning_process": {
            "type": "string",
            "description": "Representa el proceso de análisis/razonamiento que fundamenta la decisión que tomaste respecto a la agrupación de los datos en la sección 'Datos para homologar'"
        },
    "grouping_dict": {
            "type": "object",
            "description": "Esta llave contiene un JSON dónde cada llave representa el valor mostrado en la sección 'Datos para homologar' y cada valor asociado a dicha llave corresponde con el titulo del grupo asignado.",
        }
}
10. Es sumamente importante que te enfoques en generar un JSON String válido, pues la idea es usar lenguaje Python y la función json.loads para obtener un diccionario de python a partir de tu respuesta.
 
A continuación te muestro un ejemplo de la actividad:
 
Supongamos que los datos para la homolagar son los siguientes:
 
Entonces el resultado debe ser:
 
{
    "reasoning_process": "Analicé e interprete lo valores de la sección Datos para homologar y he construído el diccionario que homologa los datos.",
    "grouping_dict": {
        "tit.": "Empleado"
        "Titular": "Empleado"
        "Hija": "Hijo"
        "Hijo": "Hijo"
        "Empleado": "Empleado"
        "empleado(a)": "Empleado"
        "conyuge": "Cónyuge"
        "CONY": "Cónyuge"
    }
}
"""
 
 