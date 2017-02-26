# Spark DataFrame Cheat Sheet
Yuhao's cheat sheet for Spark DataFrame

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
#### create from seq
* `spark.createDataset(Seq(1, 2)) `
* `spark.createDataset(1 to 10)`
* `spark.createDataset(Array((1, "Tom"), (2, "Jerry"))).toDF("id", "name")`
*  `val rdd = sc.parallelize(1 to 5)`
   `spark.createDataset(rdd)`
*  `List("a").toDS()`  
   `Seq(1, 3, 5).toDS()`
* // define case class Person(name: String, age: Long) outside of the method. [reason](https://issues.scala-lang.org/browse/SI-6649)   
  `val caseClassDS = Seq(Person("Andy", 32)).toDS()`
  
* `val caseClassDS = spark.createDataset(Seq(Person("Andy", 32), Person("Andy2", 33)))`
   
   
#### create from RDD 
   
*  `val rdd = sc.parallelize(1 to 5)`       
   `rdd.toDS().show()`
  
* // define case class Person(name: String, age: Long) outside of the method. [reason](https://issues.scala-lang.org/browse/SI-6649)   
  ```
  val peopleDF = spark.sparkContext
  .textFile("examples/src/main/resources/people.txt")
  .map(_.split(","))
  .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
  .toDF()
  ```


#### create from File    
* `spark.read.json("examples/src/main/resources/people.json")`

*  
  ```
   val path = "examples/src/main/resources/people.json"`
  val peopleDS = spark.read.json(path).as[Person]
  ```


### Select

 * `df.select($"name", $"age" + 1).show()`

 * `df.createOrReplaceTempView("people")`
   `val sqlDF = spark.sql("SELECT * FROM people")`
 
 * `df.select($"name", $"age" + 1).show()`
 
 * `val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")`
 
### Filter
 
 * `df.filter($"age" > 21).show()`
 
###  GroupBy

 * `df.groupBy("age").count().show()`
 
###  Temp View and Table

* `df.createGlobalTempView("people")`

* `// Global temporary view is tied to a system preserved database `global_temp``
  `spark.sql("SELECT * FROM global_temp.people").show()`
  
* `// Global temporary view is cross-session`
  `spark.newSession().sql("SELECT * FROM global_temp.people").show()`
 

### UDF


### Schema

df.printSchema()

```
    val rows = parentModel.freqItemsets.map(f => Row(f.items, f.freq))

    val schema = StructType(Seq(
      StructField("items", dataset.schema($(featuresCol)).dataType, nullable = false),
      StructField("freq", LongType, nullable = false)))
    val frequentItems = dataset.sparkSession.createDataFrame(rows, schema)
```
`import org.apache.spark.sql.functions._`
`data.where(col($(featuresCol)).isNotNull)`






