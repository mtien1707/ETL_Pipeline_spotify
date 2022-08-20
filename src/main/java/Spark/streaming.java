package Spark;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class streaming {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spotify Streaming")
//                .master("local")
                .config("spark.dynamicAllocation.enabled","false")
                .config("spark.master", "yarn")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        ReadStream(spark);
    }

    private static void ReadStream(SparkSession spark) throws TimeoutException, StreamingQueryException {

        StructType sc = new StructType().add("title", "STRING").
                add("rank", "STRING").
                add("date", "STRING").
                add("artist", "STRING").
                add("url", "STRING").
                add("region", "STRING").
                add("chart", "STRING").
                add("trend", "STRING").
                add("streams", "STRING")
                ;

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "172.17.80.28:9092")
                .option("subscribe", "spotify_tiencm8")
                .option("startingOffsets","earliest")
                .option("auto.offset.reset","true")
                .option("failOnDataLoss", "false")
                .load();
        Dataset<Row> df1 = df.selectExpr("CAST (value AS STRING)");


        Dataset<Row> df2 = df1.withColumn("value", functions.from_json(df1.col("value"), sc ,new HashMap<>()));
        df2.printSchema();

        Dataset<Row> df4 = df2.select(df2.col("value.title"),
                df2.col("value.rank"),
                df2.col("value.date"),
                df2.col("value.artist"),
                df2.col("value.url"),
                df2.col("value.region"),
                df2.col("value.chart"),
                df2.col("value.trend"),
                df2.col("value.streams"));

        df4.printSchema();

        StreamingQuery query = df4
                .writeStream()
                .format("parquet")
                .outputMode("append")
                .option("checkpointLocation", "hdfs://172.17.80.21:9000/user/tiencm8/final/checkpoint")
                .option("path", "hdfs://172.17.80.21:9000/user/tiencm8/final/output")
//                E:\ETL_pipeline_spotify\data\output
                //E:\ETL_pipeline_spotify\data\checkpoint
                //hdfs://172.17.80.21:9000/user/tiencm8/final/checkpoint
                //hdfs://172.17.80.21:9000/user/tiencm8/final/output
                .start();
        query.awaitTermination();

    }
}
