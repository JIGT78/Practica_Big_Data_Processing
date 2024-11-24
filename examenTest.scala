package examen

import job.examen.examen.{ejercicio2, ejercicio3, ejercicio5}
import utils.TestInit
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{asc, avg, col, lit, udf}

class examenTest extends TestInit{

  import spark.implicits._

  "ejercicio1" should "mostrar esquema del DF, filtrar estudiantes (cal > 8), seleccionar nombres y ordenarlos" in {

  //Definimos una secuencia de tuplas con información sobre estudiantes (nombre,edad,calificacion)

  val in = Seq(
    ("Juan", 20, 9.5),
    ("Ana", 22, 7.8),
    ("Luis", 19, 8.2),
    ("María", 21, 9.0),
    ("Carlos", 23, 6.5)
  ).toDF("nombre", "edad", "calificacion")

  }

  "ejercicio2" should "crear columna indicando con un 1 si el número es par" in {

    val spark: SparkSession = SparkSession.builder()
      .appName("Ejercicio2Test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val inData = Seq(1, 3, 5, 4, 6, 20).toDF("Numeros")

    val outData: DataFrame = ejercicio2(inData)(spark)

    assert(outData.columns.contains("EsPar"), "La columna 'EsPar' no fue creada en el DataFrame resultante")

    val expectedData = Seq(
      (1, 0),
      (3, 0),
      (5, 0),
      (4, 1),
      (6, 1),
      (20, 1)
    ).toDF("Numeros", "EsPar")

    assert(outData.except(expectedData).isEmpty, "Los valores en el DataFrame resultante no son correctos")
  }

  "ejercicio3" should "Hacer un join con dos dataframes y calcular promedio de calificaciones" in {
    import spark.implicits._

    val estudiantes = Seq((1,"Andres"),(2,"Raquel"),(3,"Javier")).toDF("id","nombre")

    val calificaciones = Seq(
      (1, "Dibujo", 6.3),
      (2, "Dibujo", 7.3),
      (3, "Dibujo", 8.3),
      (1, "Fisica", 5.3),
      (2, "Fisica", 7.5),
      (3, "Fisica", 8.0),
      (1, "Matematicas", 8.3),
      (2, "Matematicas", 3.5),
      (3, "Matematicas", 9.0)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    val estudiantesCalificaciones = ejercicio3(estudiantes,calificaciones)

    estudiantesCalificaciones.show()

  }

  "ejercicio4" should "Crear RDD a partir de lista de palabras y contar ocurrencias" in {
    val sc = spark.sparkContext
    import spark.implicits._

    val listaPalabras = List(
      "Perro", "Gato", "Perro", "León", "Tigre",
      "Elefante", "Gato", "Jirafa", "Tigre", "Gato", "Gato", "Hipopótamo",
      "Cebra", "Perro", "Perro", "Cocodrilo", "Delfín", "Ballena", "Elefante",
      "Perro", "Lobo", "Elefante", "Elefante", "Ballena", "Rinoceronte", "Águila"
    )

    val rdd = sc.parallelize(listaPalabras)

    val df = rdd.toDF("palabra")

    val recuentoPalabras = df.groupBy("palabra")
      .agg(functions.count("palabra").alias("recuento"))
      .orderBy(functions.col("recuento").desc)

    val resultado = recuentoPalabras.collect()

    assert(resultado(0).getAs[Long]("recuento") == 5)
    assert(resultado(1).getAs[Long]("recuento") == 4)
    assert(resultado(2).getAs[Long]("recuento") == 4)

    assert(resultado(0).getString(0) == "Perro")
    assert(resultado(1).getString(0) == "Gato")
    assert(resultado(2).getString(0) == "Elefante")

    recuentoPalabras.show()
  }

  "ejercicio5" should "Cargar CSV y calcular total por producto" in {

    val spark: SparkSession = SparkSession.builder()
      .appName("Ejercicio2Test")
      .master("local[*]")
      .getOrCreate()

    val ventas = spark.read.option("header", "true").csv("src/test/resources/examen/ventas.csv")
    ventas.show()

    val resultado = ejercicio5(ventas)(spark)
    resultado.show()

    assert(resultado.filter(col("id_producto") === 101).select("total").collect()(0).getDouble(0) === 460.0)
  }
}
