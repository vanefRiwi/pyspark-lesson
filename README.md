# Libreto — Apache Spark con Python
### Palabra por palabra, diapositiva por diapositiva
[Link Colab](https://colab.research.google.com/drive/1J0IzHddzBLUP8_L5caS3aymOM_UO-V1P?usp=sharing)
[Link Slides](https://docs.google.com/presentation/d/1iGRXCdUZUa4JGP7-keVzQu7s-q-nxGTe/edit?usp=sharing&ouid=117102797785147973295&rtpof=true&sd=true)
> **Cómo leer esto:**
> Lo que está en **>** es lo que dices, tal cual, en voz alta.
> Lo que está en *cursiva entre corchetes* son acotaciones: qué hacer, dónde pausar, qué señalar.
> Los tiempos son acumulados: si vas retrasada, mira dónde recortar en la última sección.
>
> **Nada de esto hay que memorizarlo.** Léelo tres veces en voz alta y quédate con las frases ancla (las marcadas con 🔑). Esas sí, de memoria.

---

## SLIDE 1 · Portada — `0:00 → 0:40`

*[Antes de hablar: respira, mira al público dos segundos. No empieces hablando de espaldas mientras acomodas el computador.]*

> Voy a empezar con una situación que probablemente varios aquí han vivido.
>
> Es lunes. Te llega un CSV de ventas de 500 megas. Abres tu notebook, `pandas`, `read_csv`, y funciona. Perfecto.
>
> Es martes. El área de negocio te manda el histórico completo: 500 gigas. Mismo código, mismo `read_csv`… y el proceso se muere.

*[Pausa.]*

> Lo que voy a explicar hoy es la herramienta que resuelve exactamente ese martes. Se llama Apache Spark.

---

## SLIDE 2 · El problema — `0:40 → 1:40`

*[Señala el lado izquierdo, después el derecho.]*

> ¿Por qué se muere? Porque pandas trabaja así: abre el archivo, lo mete **completo** en la memoria RAM de tu computador, y lo procesa con **un solo núcleo** del procesador.
>
> Entonces el límite de pandas no es que sea lento. El límite es físico: si el archivo pesa más que tu RAM, no hay nada que hacer. No hay optimización que te salve.

🔑 > **Spark no hace que una máquina sea más rápida. Reparte el trabajo entre muchas.**

> Y esa distinción es toda la clase, en una frase.

---

## SLIDE 3 · El viaje del dato — `1:40 → 3:10`

> Antes de procesar hay que mover. Y esa parte —llevar el dato desde donde nace hasta donde se analiza— es lo que se llama ingeniería de datos.

*[Recorre las cajas con la mano, de izquierda a derecha.]*

> Los datos nacen en bases de datos transaccionales, en APIs, en logs, en archivos sueltos. Se **ingestan**, o sea, se extraen y se copian hacia un almacén central. Ese almacén hoy se llama **data lake** y ahí los datos se guardan crudos, tal como llegaron.
>
> Después alguien los limpia, los transforma, los agrega. Y solo al final salen convertidos en un dashboard, en un reporte o en un modelo de machine learning.

*[Señala la caja naranja.]*

> Spark vive **aquí**. En la etapa de procesamiento. Es el motor que hace las transformaciones pesadas.

*[Bajas a los tres conceptos de abajo. Rápido, sin profundizar.]*

> Tres términos que van a escuchar siempre alrededor de esto.
>
> **ETL contra ELT**: ETL transforma antes de guardar; ELT guarda crudo y transforma después. Hoy se usa ELT, porque el almacenamiento salió barato y el procesamiento es lo caro.
>
> **Bronze, silver, gold**: es cómo se organizan las capas del lake. Crudo, limpio, y agregado para el negocio.
>
> Y **orquestación**: Airflow decide *cuándo* corre cada proceso. Spark decide *cómo* se procesa. No compiten, se complementan.

---

## SLIDE 4 · Qué es Spark — `3:10 → 4:00`

> Entonces, en una frase: Spark es un motor que divide un trabajo grande de datos en pedazos pequeños y los ejecuta al mismo tiempo en varios computadores.
>
> Cuatro razones por las que se volvió el estándar de la industria: es distribuido, procesa en memoria en vez de escribir a disco en cada paso, sirve para SQL, streaming y machine learning con el mismo motor, y se puede usar desde Python, SQL, Scala o Java.
>
> Nosotros lo vamos a ver desde Python.

---

## SLIDE 5 · Spark, PySpark y Java — `4:00 → 4:50`

*[Esta slide contesta la duda que TODO el mundo tiene y nadie pregunta. Dila con seguridad.]*

> Y aquí quiero aclarar algo que confunde a todo el que empieza, porque los nombres engañan.
>
> **Apache Spark** es el motor. Está escrito en Scala y corre sobre Java.
> **PySpark** es la librería de Python que le da órdenes a ese motor. Es el control remoto.

*[Pausa breve.]*

> ¿Entonces hay que descargar dos cosas? No.

🔑 > **`pip install pyspark` descarga las dos.** El paquete de pip trae el motor completo empaquetado adentro. Son unos 300 megas, por eso se demora. No hay que ir a la página de Apache, ni descomprimir nada, ni configurar variables de entorno.

> Lo único que se instala aparte es Java, porque el motor corre sobre Java. Y en Google Colab, Java ya viene puesto.

---

## SLIDE 6 · Driver y executors — `4:50 → 5:50`

*[Dibuja con las manos: una mano arriba que reparte, tres abajo que trabajan.]*

> ¿Cómo hace Spark para repartir? Con dos papeles.
>
> Arriba está el **driver**: es tu script de Python. El driver arma el plan y reparte tareas, pero **no procesa datos él mismo**. Es el que dirige.
>
> Abajo están los **executors**: son los que sí hacen el trabajo.
>
> Y el archivo grande, antes de nada, se corta en pedazos que se llaman **particiones**. Cada executor procesa las suyas al mismo tiempo que los demás.

🔑 > **Esa es toda la magia. No es magia, es división del trabajo.**

*[Opcional si vas bien de tiempo:]*
> Y lo interesante es que a Spark le da igual si los executors son los ocho núcleos de mi portátil o cincuenta servidores en la nube. El código que yo escribo es exactamente el mismo.

---

## SLIDE 7 · Transformaciones vs acciones — `5:50 → 7:00`

*[Este es el concepto más importante de la clase. Bájale a la velocidad.]*

> Ahora el concepto que de verdad hay que entender de Spark, y es el que más choca al principio.
>
> **Spark no ejecuta nada hasta que se lo exiges.**
>
> Las operaciones se dividen en dos tipos. Las de la izquierda —`select`, `filter`, `withColumn`, `groupBy`, `join`— se llaman **transformaciones**, y no ejecutan absolutamente nada. Solo van anotando pasos en una libretica.
>
> Las de la derecha —`show`, `count`, `write`— se llaman **acciones**, y esas sí disparan la ejecución de todo el plan acumulado.
>
> O sea que yo puedo encadenar veinte transformaciones seguidas y no se ha leído ni un byte del archivo. Solo cuando llamo una acción, Spark dice "ah, ahora sí" y ejecuta todo de una vez.

*[Pausa. Ahora el "para qué".]*

> ¿Y para qué sirve esa pereza? Porque al ver la receta completa antes de cocinar, Spark la puede reordenar para hacer menos trabajo. Si mi cadena termina pidiendo dos columnas, el optimizador —que se llama **Catalyst**— nunca lee las otras treinta del disco.
>
> Pandas no puede hacer eso, porque ejecuta línea por línea, sin saber qué le vas a pedir después.

*[Analogía, si el público se ve perdido:]*
> Es la diferencia entre cocinar cada ingrediente apenas lo sacas de la nevera, y leer toda la receta primero para cocinar en el orden más eficiente.

---

## SLIDE 8 · Los verbos del DataFrame — `7:00 → 7:50`

> La estructura con la que se trabaja se llama **DataFrame**: es una tabla repartida entre varias máquinas, donde cada columna tiene nombre y tipo.
>
> Y la buena noticia es que la API se siente como SQL. `select` para elegir columnas, `filter` para filtrar filas, `withColumn` para crear una columna nueva, `groupBy` para agrupar, `join` para unir tablas.
>
> Lo único raro al principio es que las columnas se referencian con `F.col`, de `functions`.

🔑 > Y ojo con esto: **los DataFrames son inmutables.** Cada transformación devuelve uno nuevo, nunca modifica el anterior. No existe el `inplace=True` de pandas.

---

## SLIDE 9 · Son dos APIs distintas — `7:50 → 8:40`

> Y ya que menciono pandas: son dos APIs diferentes. Un DataFrame de Spark **no entiende** los comandos de pandas, y al revés tampoco.

*[Señala los pares de la tabla, uno o dos, no todos.]*

> Hacen lo mismo, se escriben distinto. `df.head()` en pandas es `df.show()` en Spark. El `groupby` de pandas es `groupBy` con `agg` en Spark.
>
> ¿Y cómo se conectan los dos mundos? Con `toPandas`. Spark agarra su resultado, lo trae a la memoria de una sola máquina, y te devuelve un DataFrame de pandas de verdad, con el que ya puedes graficar.

⚠️ > Pero cuidado: **`toPandas` carga todo en la RAM.** Solo se usa sobre resultados ya agregados y pequeños. Es el último paso del proceso, nunca el primero.

---

## SLIDE 10 · pandas vs Spark — `8:40 → 9:20`

*[No leas la tabla entera. Señala tres filas máximo.]*

> Resumiendo las diferencias de fondo: escala, forma de ejecutar, y mutabilidad. Y una que sorprende: **en Spark no existe el índice.** Nada de `.loc` ni `.iloc`. Como las filas están regadas entre máquinas, no hay un orden natural que indexar.
>
> Pero la conclusión importante no es cuál es mejor.

🔑 > **pandas para explorar, Spark para producir.** De hecho el flujo real es usar Spark para procesar los 500 gigas, y traer el resultado agregado —que ya son dos megas— a pandas para graficarlo.

---

## SLIDE 11 · Lo que casi nadie advierte — `9:20 → 10:10`

*[Esta slide es tu diferenciador. Dila con tono de "les voy a contar algo que no está en los tutoriales".]*

> Cuatro cosas que los tutoriales no dicen.
>
> **Uno, y es el más contraintuitivo: con datos pequeños, Spark es MÁS LENTO que pandas.** Levantar la máquina virtual de Java tarda segundos, siempre. Ese costo fijo solo se justifica con volumen. La regla informal es: menos de cinco gigas, usa pandas.
>
> **Dos: el shuffle.** Cuando haces un `groupBy` o un `join`, Spark tiene que mover datos entre executors para juntar las filas que coinciden. Eso viaja por la red y es la operación más costosa que existe. Es la respuesta al noventa por ciento de los "¿por qué mi proceso se demoró cuarenta minutos?".
>
> **Tres: no escriban funciones de Python fila por fila.** Se llaman UDFs. El motor está en Java, entonces una UDF obliga a que cada fila cruce de Java a Python y vuelva. El rendimiento se cae al piso. Si existe una función de `pyspark.sql.functions` que hace lo que necesitas, úsala siempre.
>
> **Y cuatro: hay un panel para espiar lo que hace.** Mientras el proceso corre, en `localhost:4040` está el plan de ejecución, los tiempos por etapa y las particiones. Es como se depura un proceso lento.

---

## SLIDE 12 · Dónde lo pruebo — `10:10 → 10:50`

> ¿Y esto dónde se prueba, sin tener un clúster?
>
> **Google Colab** es lo más fácil: Java ya viene instalado, entonces es una celda con `pip install pyspark` y listo. Lo único que no puedes es abrir el panel de monitoreo, porque el servidor está en la nube de Google, no en tu máquina.
>
> **En tu máquina con VS Code** sí tienes el panel, pero hay que instalar Java a mano.
>
> Y **Databricks** —que es la empresa que fundaron los creadores de Spark— tiene una edición Community gratis con un clúster real. Ahí ni siquiera creas la sesión: la variable `spark` ya existe cuando abres el notebook.

🔑 > Y lo importante: **el código es idéntico en los tres.** Lo único que cambia es una línea: `local[*]` cuando corres en tu máquina, la dirección del clúster cuando corres en producción. Aprendes con cinco mil filas en tu portátil y el mismo código corre con quinientos gigas en la nube.

---

## SLIDE 13 · Portada del ejemplo — `10:50 → 11:00`

> Y ahora sí, vamos a verlo funcionando.

*[Cambia a Colab. Ten la pestaña YA ABIERTA y la celda del `pip install` YA EJECUTADA.]*

> Tengo cinco mil ciento ochenta registros de notas de estudiantes en cinco cursos, con duplicados y con notas faltantes metidos a propósito. Y la pregunta: ¿cuál es el promedio por curso, y qué porcentaje de estudiantes aprueba?

---

## SLIDES 14-16 · La demo — `11:00 → 13:40`

*[Ve corriendo celda por celda. No leas el código en voz alta: explica qué hace y por qué.]*

**Paso 1 — la sesión:**
> La `SparkSession` es la puerta de entrada. `local[*]` significa "usa todos los núcleos de esta máquina". Esta es la línea que cambiaría en producción.

**Paso 2 — leer:**
> Uso `inferSchema` para que Spark adivine los tipos, que es lo cómodo cuando uno está explorando.

*[Y aquí metes el matiz que demuestra criterio:]*
> En producción no se hace así: inferir obliga a Spark a leer el archivo **dos veces**, una para adivinar y otra para cargar. Con cinco mil filas ni se nota; con quinientos gigas estás pagando el doble de lectura. Allá el esquema se declara a mano.

*[Corre la celda del `printSchema` y el `count`.]*
> Fíjense en algo: la celda que definió la lectura terminó instantáneamente. Esta, que tiene `count`, se demoró. Porque `count` es una acción.

---

### 🥇 Paso 3 — la limpieza: DESPACIO, este es el momento fuerte

*[Este paso dejó de ser trámite. Es donde demuestras criterio analítico, no solo manejo de sintaxis. Dale sus cuarenta segundos.]*

*[Primero corre la celda que muestra el promedio de cada curso.]*

> Antes de rellenar los nulos, miren esto. Cálculo promedia dos punto nueve. Inglés promedia cuatro punto tres. El promedio general de todos los cursos juntos es tres punto seis siete.

*[Pausa. Deja que el número se asiente.]*

> Entonces, si yo relleno una nota faltante de Cálculo con el promedio general, le estoy **regalando casi un punto** a ese estudiante. Y peor: estoy inflando el resultado del curso más difícil, que es justo el que me interesaba medir.

🔑 > **Cuando los grupos se comportan distinto, se imputa por grupo, no en global.**

*[Ahora sí corre la celda de la limpieza.]*

> Para eso uso dos cosas. `Window.partitionBy` es como un `groupBy` que **no colapsa las filas**: un `groupBy` me convertiría cinco mil filas en cinco, mientras que la ventana deja las cinco mil intactas pero cada una puede consultar el promedio de su propio curso.
>
> Y `coalesce`, que viene de SQL, significa "devuélveme el primer valor que no sea nulo": si la nota existe la deja, si es nula mete el promedio del curso.

*[Señala el resultado.]*

> Pasamos de cinco mil ciento ochenta filas a cinco mil, y de doscientas dos notas vacías a cero.

*[Detalle fino, solo si vas bien de tiempo. Suma mucho:]*
> Y un detalle de orden: el `dropDuplicates` va **antes** de calcular el promedio. Si lo dejo después, las ciento ochenta filas duplicadas pesan doble y me sesgan el cálculo.

---

**Paso 4 — transformar:**
> Creo la columna `estado`, que dice si aprobó o reprobó según si la nota llega a tres, y la columna `dedicacion`, que clasifica las horas de estudio en alta, media y baja. Uso `when` y `otherwise`, que es el `CASE WHEN` de SQL.

**Paso 5 — unir:**
> La tabla de notas solo trae el código del curso. El nombre y el área están en otra tabla, así que las uno con un `join`. Y ojo: aquí ocurre un shuffle.

**Paso 6 — la respuesta:**
> `groupBy` por área y curso, y calculo el promedio, cuántos estudiantes hay y el porcentaje de aprobación.

*[Si te preguntan cómo sacaste el porcentaje:]*
> El truco es convertir la condición en unos y ceros con `when`. El promedio de unos y ceros **es** la proporción, y multiplicado por cien da el porcentaje.

*[Muestra la salida.]*
> Ahí está: Inglés aprueba el noventa y siete por ciento de los estudiantes, Cálculo apenas el cuarenta y ocho.

**Paso 7 — guardar:**
> Y lo guardo en Parquet, no en CSV. Parquet es columnar y comprimido: pesa cinco a diez veces menos, conserva los tipos de dato y puede leer solo las columnas que le pidas sin tocar el resto del archivo. Es el formato estándar del data lake.

---

## SLIDE 17-18 · Los cuadros de referencia — `13:40 → 14:05`

*[Estas NO se explican. Se ponen en pantalla y se anuncian.]*

> Les dejo dos cuadros de referencia. Este tiene los conceptos de Spark con qué es cada uno y por qué importa.

*[Cambias.]*

> Y este es una chuleta de sintaxis: la misma operación escrita en pandas y en Spark, lado a lado. Si vienen de pandas, con esta tabla arrancan.

*[Déjala unos segundos en pantalla en silencio. La gente va a querer fotografiarla.]*

---

## SLIDE 19 · Cierre — `14:05 → 14:35`

> Para cerrar, cuatro ideas.
>
> Los datos se mueven antes de poder analizarse: eso es ingeniería de datos. Se procesan en paralelo, partiendo el archivo en particiones. El plan se optimiza antes de ejecutarse, y por eso Spark es perezoso a propósito. Y pandas y Spark no compiten: conviven.

🔑 > Si se llevan una sola frase, que sea esta: **pandas para explorar, Spark para cuando el dato ya no cabe en una máquina.**

> Muchas gracias. ¿Preguntas?

---
---

# Cómo destacar (estás compitiendo)

Todos los demás van a explicar qué es Spark. La diferencia no va a estar en el contenido, va a estar en tres cosas: **demostrar en vez de afirmar, saber lo que otros no saben, y controlar el nervio.**

## 1. La demostración que nadie más va a hacer 🥇

Este es tu as. Todo el mundo va a *decir* que Spark optimiza el plan. Tú lo vas a **probar en pantalla**.

Al final de la demo, corre esto:

```python
plan = (notas
        .dropDuplicates(["id_registro"])
        .filter(F.col("horas_estudio") > 5)
        .groupBy("id_curso")
        .agg(F.avg("nota").alias("promedio")))

plan.explain()
```

En la salida busca la línea que empieza con `FileScan csv`. Vas a ver algo así:

```
FileScan csv [id_registro, id_curso, nota, horas_estudio]
```

Y ahí dices:

> Fíjense en esto. El archivo tiene **cinco** columnas, pero el plan dice que Spark solo va a leer **cuatro**: dejó por fuera el nombre del estudiante, porque mi consulta no lo necesita. Yo nunca le dije que hiciera eso. El optimizador se dio cuenta solo, porque vio la receta completa antes de cocinar. **Eso** es para lo que sirve la evaluación perezosa.

Es un momento de treinta segundos que convierte un concepto abstracto en evidencia visible. Practícalo hasta que te salga sin buscar en la pantalla.

## 2. El segundo momento que te separa: la decisión de limpieza 🥈

Todos los demás van a limpiar los nulos con el promedio general, porque es lo que sale en cualquier tutorial. Tú vas a mostrar en pantalla **por qué eso está mal** antes de hacerlo bien.

Corre primero la celda que muestra el promedio de cada curso (2.9 en Cálculo contra 4.3 en Inglés) y después el promedio general (3.67). El contraste habla solo. Ahí dices:

> Rellenar una nota faltante de Cálculo con el promedio general le regala casi un punto a ese estudiante, e infla justo el curso que quería medir.

La diferencia entre saber usar `fillna` y saber **cuándo no usarlo** es exactamente la diferencia entre alguien que ejecuta y alguien que analiza. Y el jurado la nota.

Si además alcanzas a mencionar que el `dropDuplicates` va antes del cálculo del promedio para que los duplicados no sesguen el resultado, cierras el punto redondo.

## 3. Di en voz alta lo que suena mal

La gente que está aprendiendo vende su herramienta. La gente que sabe reconoce sus límites. Cuando digas:

> Con datos pequeños Spark es **más lento** que pandas, y usarlo para un CSV de cinco mil filas sería un error de criterio.

…acabas de sonar como alguien que trabajó con esto, no como alguien que vio un tutorial. Es contraintuitivo, es honesto, y el jurado lo va a registrar. **No te saltes esta frase aunque vayas apurada.**

## 4. Prepara las tres preguntas difíciles

Casi seguro sale una de estas. Tener la respuesta lista, corta y segura, vale más que diez minutos de exposición:

| Pregunta | Tu respuesta |
|---|---|
| *¿Por qué es rápido si Python es lento?* | Porque el motor está en Scala sobre la JVM. Python solo manda instrucciones; los datos nunca pasan por Python. La excepción son las UDFs, y por eso se evitan. |
| *¿Spark reemplaza a pandas?* | No, conviven. Spark agrega los 500 GB, pandas grafica el resumen de 2 MB. |
| *¿Qué es un RDD?* | La API original de bajo nivel. Hoy se usa DataFrame porque pasa por el optimizador Catalyst y rinde mejor. El RDD sigue ahí por debajo. |
| *¿Necesito un clúster?* | No. `local[*]` corre en tu portátil y el código es el mismo. |
| *¿Por qué se demoró tanto ese job?* | Casi siempre un shuffle: un `groupBy` o un `join` moviendo datos entre executors. |

Si te preguntan algo que no sabes, la respuesta correcta es: **"No lo sé con certeza, pero mi intuición es que…"**. Inventar es lo único que sí te hunde.

## 5. Detalles de ejecución que suman más de lo que parecen

- **Corre la demo una vez antes de exponer.** El primer arranque de la JVM tarda segundos y no quieres ese silencio en vivo. Si es Colab, deja el `pip install` ya ejecutado.
- **Ten la ventana del código ya abierta** en otra pestaña. Buscar archivos en vivo mata el ritmo.
- **Plan B por escrito:** si la demo falla, las slides 14 a 16 tienen el código y la salida real. Dilo sin drama: *"se me cayó el entorno, vamos por las diapositivas"* y sigues. Lo que se nota mal no es que falle, es que te desarmes.
- **No leas las diapositivas.** Están diseñadas para que las señales, no para que las recites. Si te descubres leyendo, la audiencia deja de escucharte porque ya leyó más rápido que tú.
- **Cronométrate en voz alta, dos veces.** Es la única forma de saber tu tiempo real. Casi todo el mundo tarda 30-40% más de lo que cree.

## 6. Si tienes que recortar

En orden de qué sacrificar primero:

1. La analogía de la receta (slide 7)
2. El bloque de ETL/ELT y bronze-silver-gold (slide 3) — lo mencionas en diez segundos y sigues
3. Las slides 17 y 18 — las pasas anunciándolas, sin explicar
4. Los pasos 4 y 5 de la demo — vas directo de la limpieza a la agregación

**Lo que NO se recorta nunca:** transformaciones vs acciones (slide 7), el paso 3 de la limpieza, la demo corriendo, y el `explain()`. Ese es tu núcleo.
