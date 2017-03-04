# Spark DataFrame Cheat Sheet
Yuhao's cheat sheet for Spark DataFrame. Glad if it can help and welcome to contribute. 

## Core Concepts
DataFrame is simply a type alias of Dataset[Row]


## Quick Reference

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._


### Creation
* create DataSet from seq

  ```
    // vertically
    spark.createDataset(Seq(1, 2))
  ```
  
  ```
    // horizontally
    val rows = spark.sparkContext.parallelize(Seq(Row.fromSeq(Seq(1, 2))))
    val schema = StructType(Seq("col1", "col2").map(col => StructField(col, IntegerType, nullable = false)))
    spark.createDataFrame(rows, schema).show()
  ```
  
* create DataSet from range

  ```
    spark.createDataset(1 to 10)
  ```
    
* create DataSet from array of tuples

  ```
    spark.createDataset(Array((1, "Tom"), (2, "Jerry"))).toDF("id", "name")
  ```
    
* Seq to Dataset

  ```
    List("a").toDS()
    Seq(1, 3, 5).toDS()
  ```
   
* create Dataset from Seq of case class

   // define case class Person(name: String, age: Long) outside of the method. [reason](https://issues.scala-lang.org/browse/SI-6649)
  ```
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    val caseClassDS = spark.createDataset(Seq(Person("Andy", 32), Person("Andy2", 33)))
  ```
   
* create Dataset from RDD 

  ```
    import spark.implicits._
    val rdd = sc.parallelize(1 to 5)
    spark.createDataset(rdd)
  ```
   
  ```
    import spark.implicits._
    val rdd = sc.parallelize(1 to 5)
    rdd.toDS().show()
    rdd.toDF().show()
  ```
  
  ```
    val df = rdd.map({ 
    case Row(val1: String, ..., valN: Long) => (val1, ..., valN)}).toDF("col1_name", ..., "colN_name")
  ```
  
    // define case class Person(name: String, age: Long) outside of the method. [reason](https://issues.scala-lang.org/browse/SI-6649)  
  ```   
    val peopleDF = spark.sparkContext
     .textFile("examples/src/main/resources/people.txt")
     .map(_.split(","))
     .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
     .toDF()
  ```
* create DataFrame from RDD with schema

  ```
    val rows = freqItemsets.map(f => Row(f.items, f.freq))
    val schema = StructType(Seq(
      StructField("items", dataset.schema($(featuresCol)).dataType, nullable = false),
      StructField("freq", LongType, nullable = false)))
    val frequentItems = dataset.sparkSession.createDataFrame(rows, schema)
  ```
  
*  create DataSet from File    
  ``` 
    spark.read.json("examples/src/main/resources/people.json")
  ```

  ```
    // from json
    val path = "examples/src/main/resources/people.json"`
    val peopleDS = spark.read.json(path).as[Person]
  ```
  
  ```
    // from text file
    import spark.implicits._
    val dataset = spark.read.textFile("data/mllib/sample_fpgrowth.txt")
      .map(t => t.split(" ")).toDF("features")
  ```
  
  ```
    // read from csv
    val df = session.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .csv("csv/file/path")
  ```      


### Select

* select with col function

  ```
    import org.apache.spark.sql.functions._
    dataset.select(col($(labelCol)), col($(featuresCol))).rdd.map {
      case Row(label: Double, features: Vector) =>
        LabeledPoint(label, features)
    }
  ```
  
  ```
    // avg average
    dataset.select(avg(inputCol)).as[Double].first()
  ```
  
  ```
    // median (or other percentage)
    filtered.stat.approxQuantile(inputCol, Array(0.5), 0.001)
  ```

* select with basic calculation
 
  ```
    df.select($"name", $"age" + 1).show()
  ```

* select from temp view

  ```
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
  ```
 
  ```
    // Global temporary view is tied to a system preserved database `global_temp
    spark.sql("SELECT * FROM global_temp.people").show()
  
    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
  ```
   
* select with sql
 
  ```
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
  ```
 
* Filter
 
  ```
    df.filter($"age" > 21).show()
  ```      
 
* GroupBy

  ```
    df.groupBy("age").count().show()
  ```  
  
  ```
    val dfMax = df.groupBy($"id").agg(sum($"value"))
  ```
  
  ```
    df.as[Record]
      .groupByKey(_.id)
      .reduceGroups((x, y) => x).show()
  ```
  
  
* Window

  ```
    import org.apache.spark.sql.functions.{rowNumber, max, broadcast}
    import org.apache.spark.sql.expressions.Window

    val temp = Window.partitionBy($"hour").orderBy($"TotalValue".desc)

    val top = df.withColumn("rn", rowNumber.over(temp)).where($"rn" === 1)
  ```
  
 * join 
  
  ```
    df.join(broadcast(dfMax), "col1").show()
  ```
  
  
  

### Append

* append constant
  ```
    import org.apache.spark.sql.functions._
    df.withColumn("new_column", lit(10)).show()
  ```
  
  ```
    df.withColumn("map", map(lit("key1"), lit(1), lit("key2"), lit(2)))
  ```

### UDF


* select DataFrame with UDF

  ```
    protected def raw2prediction(rawPrediction: Vector): Double = rawPrediction.argmax
    ...
    
    udf(raw2prediction _).apply(col(getRawPredictionCol))
  ```

  ```
    val predictUDF = udf { (features: Any) =>
      bcastModel.value.predict(features.asInstanceOf[Vector])
    }
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  ```
  
  
### Schema

* print schema

  ```
    df.printSchema()
  ```
  ```
    dataset.schema($(labelCol))
  ```
  
  ```
  // spark internal
  SchemaUtils.checkColumnTypes(schema, inputCol, Seq(DoubleType, FloatType))
  ```

  ```
    MetadataUtils.getNumClasses(dataset.schema($(labelCol)))
  ```

### Read and write

* parquet

  ```
    val data = sparkSession.read.format("parquet").load(dataPath)
    val Row(coefficients: Vector, intercept: Double) =
        data.select("coefficients", "intercept").head()
  ```
  
  
