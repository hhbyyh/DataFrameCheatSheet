# Spark DataFrame Cheat Sheet
Yuhao's cheat sheet for Spark DataFrame

## Core Concepts



## Quick Reference

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._


### Creation
* `spark.createDataset(Seq(1, 2)) `

* `spark.createDataset(1 to 10)`

* `spark.createDataset(Array((1, "Tom"), (2, "Jerry"))).toDF("id", "name")`

*  `val rdd = sc.parallelize(1 to 5)`
   `spark.createDataset(rdd)`
   
*  `val rdd = sc.parallelize(1 to 5)`       
   `rdd.toDS().show()`
      
*  `List("a").toDS()`  
   `Seq(1, 3, 5).toDS()`
  
* // define case class Person(name: String, age: Long) outside of the method. [reason](https://issues.scala-lang.org/browse/SI-6649)   
  `val caseClassDS = Seq(Person("Andy", 32)).toDS()`
  
* `val caseClassDS = spark.createDataset(Seq(Person("Andy", 32), Person("Andy2", 33)))`

* `spark.read.json("examples/src/main/resources/people.json")`

    



### Select

df.select($"name", $"age" + 1).show()

 df.createOrReplaceTempView("people")
 val sqlDF = spark.sql("SELECT * FROM people")
 
 df.select($"name", $"age" + 1).show()
 
 df.filter($"age" > 21).show()

### UDF


### Schema










