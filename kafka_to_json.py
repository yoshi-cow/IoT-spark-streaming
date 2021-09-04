import findspark
findspark.init('/home/yoshi-1/spark-3.1.1-bin-hadoop2.7')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    # 1. sparksession作成
    spark = SparkSession.builder.appName("KafkatoJson").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 2. 10.0.2.15:9092でアクセス可能なKafkaの、"sensor-data" Topic からデータを取得
    kafkaDataFrame = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10.0.2.15:9092")\
                                .option("subscribe", "sensor-data").load()

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
    
    # 6. パース結果をJSON形式のファイルに出力
    query = formattedDataFrame.writeStream\
                                .outputMode("append")\
                                .format("json")\
                                .option("path", "/opt/spark-book/chapter06/data/json")\
                                .option("checkpointLocation", "/opt/spark-book/chapter06/state/KafkaToJsonFile")\
                                .start()

    # 7. 終了されるまで継続的に読み込みと出力を実行
    query.awaitTermination()