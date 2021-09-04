import findspark
findspark.init('/home/yoshi-1/spark-3.1.1-bin-hadoop2.7')

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    # 1. sparksession作成
    spark = SparkSession.builder.appName("WindowAverage").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 2. 10.0.2.15:9092でアクセス可能なkafkaの、joioned-sensor-data Topic からデータを取得
    kafkaDataFrame = spark.readStream\
                            .format("kafka")\
                            .option("kafka.bootstrap.servers", "10.0.2.15:9092")\
                            .option("subscribe", "joined-sensor-data")\
                            .load()
    
    # 3. Valueカラムを文字列に変換
    stringFormattedDataFrame = kafkaDataFrame.selectExpr("CAST(value AS STRING) as value")

    # 4. JSONとして読み込むスキーマを指定
    joinedSchema = StructType()\
                    .add("sensor_id", LongType())\
                    .add("date", StringType())\
                    .add("lat", DoubleType())\
                    .add("lon", DoubleType())\
                    .add("temperature", DoubleType())\
                    .add("humidity", DoubleType())\
                    .add("ph", DoubleType())\
                    .add("whc", DoubleType())\
                    .add("field_id", StringType())

    # 5. スキーマを指定し、JSONとしてデータをパース
    jsonParsedDataFrame = stringFormattedDataFrame.select(from_json(stringFormattedDataFrame.value, joinedSchema).alias("sensor_data"))
    formattedDataFrame = jsonParsedDataFrame.select(
                            col("sensor_data.sensor_id").alias("sensor_id"),
                            col("sensor_data.field_id").alias("field_id"),
                            col("sensor_data.date").alias("date"),
                            col("sensor_data.lat").alias("lat"),
                            col("sensor_data.lon").alias("lon"),
                            col("sensor_data.temperature").alias("temperature"),
                            col("sensor_data.humidity").alias("humidity"),
                            col("sensor_data.ph").alias("ph"),
                            col("sensor_data.whc").alias("whc")
                            )

    # 6. 時刻形式を指定し、"date"カラムの時刻文字列をTimestampに変換
    timestampedDataFrame = formattedDataFrame.withColumn("timestamp", to_timestamp(formattedDataFrame.date, 'yyyy/MM/dd HH:mm:ss'))

    # 7. 分析用に必要なカラムのみ抽出
    analyzeBase = timestampedDataFrame.select("timestamp", "field_id", "temperature", "humidity", "ph", "whc")

    # 8. 上記カラムのウィンドウ処理平均を算出（スライディングウィンドウ：5分間隔、1分ずつスライド）
    windowedAvg = analyzeBase\
                    .withWatermark("timestamp", "10 minutes")\
                    .groupBy(
                        window(analyzeBase.timestamp, "5 minutes", "1 minutes"),
                        analyzeBase.field_id
                    )\
                    .avg("temperature", "humidity", "ph", "whc")

    # 9. ウィンドウの開始時刻、終了時刻をカラムとして抽出し、名称を整理
    timeExtractedAvg = windowedAvg.select(
                            "field_id",
                            col("window.start").alias("window_start"),
                            col("window.end").alias("window_end"),
                            col("avg(temperature)").alias("avg_temperature"),
                            col("avg(humidity)").alias("avg_humidity"),
                            col("avg(ph)").alias("avg_ph"),
                            col("avg(whc)").alias("avg_whc")
                        )

    # 10. 集計結果をコンソールに出力
    query = timeExtractedAvg.writeStream\
                            .outputMode("update")\
                            .format("console")\
                            .start()

    # 11. 終了されるまで継続的に読み込みと出力を実行
    query.awaitTermination()