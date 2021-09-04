import findspark
findspark.init('/home/yoshi-1/spark-3.1.1-bin-hadoop2.7')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


if __name__ == "__main__":

    # 1. sparksessionを作成
    spark = SparkSession.builder.appName("KafkaToCassandra").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 2. 10.0.2.15:9092でアクセス可能なKafkaの、"sensor-data" Topic からデータを取得
    kafkaDataFrame = spark.readStream.format("kafka")\
                            .option("kafka.bootstrap.servers", "10.0.2.15:9092")\
                            .option("subscribe", "sensor-data")\
                            .load()


    # 3. Valueカラムを文字列に変換
    stringFormattedDataFrame = kafkaDataFrame.selectExpr("CAST(value AS STRING) as value")

    # 4. JSONとして読み込むスキーマを指定
    coordSchema = StructType().add("lat", DoubleType()).add("lon", DoubleType())
    mainSchema = StructType().add("temperature", DoubleType()).add("humidity", DoubleType())\
                                .add("ph", DoubleType()).add("whc", DoubleType())
    schema = StructType().add("id", LongType()).add("date", StringType()).add("coord", coordSchema).add("main", mainSchema)

    # 5. スキーマを指定し、JSONとしてデータをパース
    jsonParsedDataFrame = stringFormattedDataFrame.select(from_json(stringFormattedDataFrame.value, schema).alias("sensor_data"))
    formattedDataFrame = jsonParsedDataFrame.select(
                                                col("sensor_data.id").alias("id"),
                                                col("sensor_data.date").alias("date"),
                                                col("sensor_data.coord.lat").alias("lat"),
                                                col("sensor_data.coord.lon").alias("lon"),
                                                col("sensor_data.main.temperature").alias("temperature"),
                                                col("sensor_data.main.humidity").alias("humidity"),
                                                col("sensor_data.main.ph").alias("ph"),
                                                col("sensor_data.main.whc").alias("whc")
                                                    )

    # 6. パーティション用にyyyyMM形式で年月を保持するカラムを追加
    withMonth = formattedDataFrame.withColumn("ts", from_unixtime(unix_timestamp("date", "yyyy/MM/dd HH:mm:ss")))\
                                    .select(date_format("ts", "yyyyMM").alias("month"), "*")
    # cassanrdaテーブルの列名に合わせる
    withMonth = withMonth.select(
                                col("id"),
                                col("month").alias("date"),
                                col("lat"),
                                col("lon"),
                                col("temperature"),
                                col("humidity"),
                                col("ph"),
                                col("whc"),
                                col("ts")
    )

    # debug コンソール出力
    # query = withMonth.writeStream.outputMode("append").format("console").start()

    # 7. パース結果をcassandraのテーブル（keyspace:imai_farm, table:sensor_raw)に出力
    query = withMonth.writeStream\
                        .format("org.apache.spark.sql.cassandra")\
                        .options(table="sensor_raw", keyspace="imai_farm")\
                        .option("checkpointLocation", "/opt/spark-book/chapter06/state/KafkaToCassandra")\
                        .outputMode("append")\
                        .start()
                        # .trigger(processingTime="5 seconds")\


    
    # 8. 終了されるまで継続的に読み込みと出力を実行
    query.awaitTermination()
