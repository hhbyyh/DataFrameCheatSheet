# Spark DataFrame Cheat Sheet
Cheatsheet for Apache Spark DataFrame.

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
  
  ```
    val newNames = Seq("id", "x1", "x2", "x3")
    val dfRenamed = df.toDF(newNames: _*)
  ```
    
* Seq to Dataset

  ```
    List("a").toDS()
    Seq(1, 3, 5).toDS()
  ```
  
  ```
    import spark.implicits._
    Seq.empty[(String, Int)].toDF("k", "v")
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
  
  ```
    val schema = StructType( StructField("k", StringType, true) ::
        StructField("v", IntegerType, false) :: Nil)
    spark.createDataFrame(sc.emptyRDD[Row], schema).show()
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
  
  ```
    // check df empty
    df.rdd.isEmpty
  ```
  
  ```
    // select array of columns
    df.select(cols.head, cols.tail: _*)
    
    df.select(cols.map(col): _*)
  ```
* select with type

  ```
    output.select("features").as[Vector].collect()
  ```

* select with basic calculation
 
  ```
    import spark.implicits._
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
  
  ```
    val ic = col(inputCol)
    val filtered = dataset.select(ic.cast(DoubleType))
      .filter(ic.isNotNull && ic =!= $(missingValue) && !ic.isNaN)
  ```
  
  ```
  df.filter($"state" === "TX") 
  df.filter("state = 'TX'")
  df.filter($"foo".contains("bar"))
  df.filter(not($"state" === "TX"))
  df.filter($"foo".like("bar"))
  ```
  
* sort

  ```
    import org.apache.spark.sql.functions._

    df.orderBy(asc("col1"))

    df.sort(desc("col2"))
  ```
  
* Rename column

  ```
      df.select($"id".alias("x1")).show()
  ```
  
  ```
    val lookup = Map("id" -> "foo", "value" -> "bar")
    df.select(df.columns.map(c => col(c).as(lookup.getOrElse(c, c))): _*)
  ```
  
  ```
    df.withColumnRenamed("_1", "x1")
  ```
  
* change column type (cast)

  ```
      val df2 = df.select($"id", col("value").cast(StringType))
  ```
  
  ```
  df.selectExpr("cast(year as int) year", 
                        "make")
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
  
  ```
    Leaddetails.join(
        Utm_Master, 
        Leaddetails("LeadSource") <=> Utm_Master("LeadSource")
            && Leaddetails("Utm_Source") <=> Utm_Master("Utm_Source")
            && Leaddetails("Utm_Medium") <=> Utm_Master("Utm_Medium")
            && Leaddetails("Utm_Campaign") <=> Utm_Master("Utm_Campaign"),
        "left"
    )
  ```
  
* concat

  ```
    df.createOrReplaceTempView("df")
    spark.sql("SELECT CONCAT(id, ' ',  value) as cc FROM df").show()
  ```  
  
  ```
    df.select(concat($"id", lit(" "), $"value"))
  ```
  
* with generic
  
  ```
    private def genericFit[T: ClassTag](dataset: Dataset[_]): FPGrowthModel = {
        val data = dataset.select($(featuresCol))
        val items = data.where(col($(featuresCol)).isNotNull).rdd.map(r => r.getSeq[T](0).toArray)
        ...
      }
   ```   
   
* when
 
  ```
     val ic = col(inputCol)
     outputDF = outputDF.withColumn(outputCol,
       when(ic.isNull, surrogate)
       .when(ic === $(missingValue), surrogate)
       .otherwise(ic)
       .cast(inputType))
  ```
   
  ```
    val coder: (Int => String) = (arg: Int) => {if (arg < 100) "little" else "big"}
    val sqlfunc = udf(coder)
    myDF.withColumn("Code", sqlfunc(col("Amt")))
  ```
  
  ```
    // (1, -1) label to (1, 0) label
    df.select($"id", when($"label" === 1, 1).otherwise(0).as("label")).show()
  ```
  
  ```
    // drop NaN and null
    df.na.drop().show()
  ```
  
* cube  
  
  ```
    ds.cube($"department", $"gender").agg(Map(
        "salary" -> "avg",
        "age" -> "max"
      ))
  ```
  
* statistics

  ```
    df.stat.freqItems(Seq("id")).show()
    df.stat.approxQuantile(...)
    df.stat.bloomFilter(...)
    df.stat.countMinSketch()
  ```
  
  ```
    // count distinct
    df.select(approx_count_distinct(col("value"))).show()
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
  
  ```
    df.select('*', (df.age + 10).alias('newAge'))
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
  
  ```
    // concat two columns with udf
    //Define a udf to concatenate two passed in string values
    val getConcatenated = udf( (first: String, second: String) => { first + " " + second } )

    //use withColumn method to add a new column called newColName
    df.withColumn("newColName", getConcatenated($"col1", $"col2")).select("newColName", "col1", "col2").show()
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
    df.explain()
  ```    
  
  ```
    // spark internal
    SchemaUtils.checkColumnTypes(schema, inputCol, Seq(DoubleType, FloatType))
  ```

  ```
    MetadataUtils.getNumClasses(dataset.schema($(labelCol)))
  ```
 
* repartition

  ```
    df.repartition($"value")
    df.explain()
  ```
  
  ```
    df.repartition(2)
  ```    

* custom class

  ```
    import spark.implicits._
    class MyObj(val i: Int)
    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[MyObj]
    // ...
    val d = spark.createDataset(Seq(new MyObj(1),new MyObj(2),new MyObj(3)))
  ```

  ```
    class MyObj(val i: Int, val u: java.util.UUID, val s: Set[String])

    // alias for the type to convert to and from
    type MyObjEncoded = (Int, String, Set[String])

    // implicit conversions
    implicit def toEncoded(o: MyObj): MyObjEncoded = (o.i, o.u.toString, o.s)
    implicit def fromEncoded(e: MyObjEncoded): MyObj =
        new MyObj(e._1, java.util.UUID.fromString(e._2), e._3)

    val d = spark.createDataset(Seq[MyObjEncoded](
      new MyObj(1, java.util.UUID.randomUUID, Set("foo")),
      new MyObj(2, java.util.UUID.randomUUID, Set("bar"))
    )).toDF("i","u","s").as[MyObjEncoded]
  ```
  

### Read and write

* parquet

  ```
    df.write.parquet(dataPath)
    ...
    val data = sparkSession.read.format("parquet").load(dataPath)
    val Row(coefficients: Vector, intercept: Double) =
        data.select("coefficients", "intercept").head()
  ```
  
* checkpoint

  ```
    df.checkpoint()
  ```
  
* save by key

  ```
      df.write.partitionBy("id").text("people")
  ```


  
