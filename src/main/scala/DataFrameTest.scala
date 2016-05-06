import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Column, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.trend.spn.{MyMLUtil, TupleUDF}

/**
 * Created by GregHuang on 5/1/16.
 */

object DataFrameTest extends App {
  val sparkConf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("LogisticRegressionML")
    .set("spark.driver.port", "7777")
    .set("spark.driver.host", "localhost")

  val sc = new SparkContext(sparkConf)
  val sqlCtx = new SQLContext(sc)

  import sqlCtx.implicits._

  val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
//  df1.write.parquet("data/test_table/key=2")

  val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
//  df2.write.parquet("data/test_table/key=3")

  val df3 = sc.makeRDD(11 to 15).map(i => (i, Vectors.dense(i + 1, i + 2, i + 3))).toDF("foo", "vector1")
//  df3.write.parquet("data/test_table/key=4")

  val dfA = sc.makeRDD(11 to 15).map(i => (i, Vectors.dense(i + 4, i + 5, i + 6))).toDF("bar", "vector2")
  val dfAll = sqlCtx.read.option("mergeSchema", "true").parquet("data/test_table")
  dfAll.printSchema()

  // Append valuse to a column, but it will be RDD
  val df5 = dfAll.filter(dfAll("key") === 4).
    map(row => Vectors.dense(row(4).asInstanceOf[Vector].toArray :+ 100.0))
  
  df5.collect().foreach(println)

  // iterate each row
  val df6 = dfAll.select(dfAll("vector1"))
    .map { r =>
      val v = r(0).asInstanceOf[Vector]
      if (v != null) {
        val data = v.toArray
        Row(data(0))
      }
  }

  df6.collect().foreach(println)

  val dfJoin = dfA.join(dfAll, $"bar" === $"foo")
  dfJoin.show()

  //val ddf = df.withColumn("newCol", TupleUDF.toTuple2[Vector, Vector].apply(df("one"), df("single")))
  val ddf = dfJoin
    .withColumn("vector", TupleUDF.mergeCol($"vector1", $"vector2"))
    .drop($"foo")

  ddf.select(ddf("vector")).collect().foreach(println)

  dfJoin.select($"foo", $"bar").distinct().show()

  sc.stop()
}
