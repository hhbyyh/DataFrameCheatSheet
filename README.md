# Spark DataFrame Cheat Sheet
Yuhao's cheat sheet for Spark DataFrame

## Core Concepts



## Quick Reference

    ```
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    ```

### Creation




### Select


### UDF


### Schema










