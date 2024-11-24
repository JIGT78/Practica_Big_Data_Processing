package job.examen

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}


object examen {

  val spark = SparkSession.builder()
    .appName("Examen_Scala")
    .master("local[*]")
    .getOrCreate()

  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
  Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
              estudiantes (nombre, edad, calificación).
            Realiza las siguientes operaciones:

            Muestra el esquema del DataFrame.
            Filtra los estudiantes con una calificación mayor a 8.
            Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  def ejercicio1(estudiantes: DataFrame)(spark:SparkSession): DataFrame = {

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    import spark.implicits._

    val estudiantes = Seq(
      ("Juan", 20, 9.5),
      ("Ana", 22, 7.8),
      ("Luis", 19, 8.2),
      ("María", 21, 9.0),
      ("Carlos", 23, 6.5))
      .toDF("nombre", "edad", "calificacion")

    estudiantes.show()
    println(estudiantes.schema)

    val estudiantesCalificacionSuperior = estudiantes.filter(col("calificacion")>8)
    estudiantesCalificacionSuperior.show()

    val estudiantesOrdenados = estudiantes.select("nombre", "calificacion").orderBy(col("calificacion").desc)
    estudiantesOrdenados.show()
    estudiantes

  }

  /**Ejercicio 2: UDF (User Defined Function)
  Pregunta: Define una función que determine si un número es par o impar.
            Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  def ejercicio2(numeros: DataFrame)(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val esParUDF = org.apache.spark.sql.functions.udf((n: Int) => if (n % 2 == 0) 1 else 0)
    val numerosClasificados = numeros.withColumn("EsPar", esParUDF($"Numeros"))
    numerosClasificados
  }
  /**Ejercicio 3: Joins y agregaciones
  Pregunta: Dado dos DataFrames,
            uno con información de estudiantes (id, nombre)
            y otro con calificaciones (id_estudiante, asignatura, calificacion),
            realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
  */
  def ejercicio3(estudiantes: DataFrame , calificaciones: DataFrame): DataFrame = {
    val joinEstudiantesCalificaciones = estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
      .select("id", "nombre", "asignatura", "calificacion")

    joinEstudiantesCalificaciones.groupBy("id").agg(avg("calificacion").as("promedio"))
  }

  /**Ejercicio 4: Uso de RDDs
  Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

  */

  def ejercicio4(palabras: List[String])(spark:SparkSession): RDD[(String, Int)] = {
    val sc = spark.sparkContext

    val listaPalabras = List(
      "Perro", "Gato", "Perro", "León", "Tigre",
      "Elefante", "Gato", "Jirafa", "Tigre", "Gato", "Gato", "Hipopótamo",
      "Cebra", "Perro", "Perro", "Cocodrilo", "Delfín", "Ballena", "Elefante",
      "Perro", "Lobo", "Elefante", "Elefante", "Ballena", "Rinoceronte", "Águila"
    )

    val rdd = sc.parallelize(listaPalabras)

    import spark.implicits._
    val df = rdd.toDF("palabra")

    val recuentoPalabras = df.groupBy("palabra")
      .agg(functions.count("palabra").alias("Recuento"))

    recuentoPalabras.show()
    recuentoPalabras.as[(String, Int)].rdd

  }
  /**
  Ejercicio 5: Procesamiento de archivos
  Pregunta: Carga un archivo CSV que contenga información sobre
            ventas (id_venta, id_producto, cantidad, precio_unitario)
            y calcula el ingreso total (cantidad * precio_unitario) por producto.
  */
  def ejercicio5(ventas: DataFrame)(spark:SparkSession): DataFrame = {

    ventas.groupBy("id_producto").agg(sum(col("cantidad") * col("precio_unitario")).alias("total"))

  }

}
