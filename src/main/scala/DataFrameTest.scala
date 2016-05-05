import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Column, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.trend.spn.TupleUDF

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

//  val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
//  df1.write.parquet("data/test_table/key=2")
//
//  val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
//  df2.write.parquet("data/test_table/key=3")

//    val df4 = sc.makeRDD(11 to 15).map(i => (i, Vectors.dense(i+1, i+2, i+3))).toDF("single", "vector")
//    df4.write.parquet("data/test_table/key=4")

  val dfA = sc.makeRDD(11 to 15).map(i => (i, Vectors.dense(i + 4, i + 5, i + 6))).toDF("one", "foo")
//

  //val dfA = sqlCtx.read.parquet

  val dfB = sqlCtx.read.option("mergeSchema", "true").parquet("data/test_table")
  dfB.printSchema()

  // Append valuse to a column
  val df5 = dfB.filter(dfB("key") === 4).map(t => Vectors.dense(t(3).asInstanceOf[Vector].toArray :+ 100.0))
  println(df5)

  val v2 = Vectors.dense(20.0, 21.0)
  val v1 = Vectors.dense(20.0, 21.0)
  val v3 = v1.toArray ++: v2.toArray

  v3.foreach(println)

  // iterate each row
  val df7 = dfB.select(dfB("vector")).map { r =>
    val v = r(0).asInstanceOf[Vector]
    val data = v.toArray
    Row(Vectors.dense(data))
  }

  df7.collect().foreach(println)

  //val df6 = df3.withColumn("vector", col)
  //df6.show()

  val df = dfA.join(dfB, $"one" === $"single")
  df.show()

  //val ddf = df.withColumn("newCol", TupleUDF.toTuple2[Vector, Vector].apply(df("one"), df("single")))
  val ddf = df.withColumn("newCol", TupleUDF.mergeCol(df("foo"), df("vector"))).drop($"foo").drop($"vector")
  ddf.select(ddf("newCol")).collect().foreach(println)

  sc.stop()
}
