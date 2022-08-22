# ETL_pipeline_spotify
# Chức năng hệ thống

- Streaming, visualization dữ liệu spotify hằng ngày

# mô hình hoạt động 
![image](https://user-images.githubusercontent.com/88629792/184476407-239dfb31-628a-40be-8f47-6d98aff3b7df.png)
# dữ liệu :
![image](https://user-images.githubusercontent.com/88629792/185884758-cf2666dd-535f-4bd0-9478-970525665723.png)

![Uploading image.png…]()

Câu lệnh start Spark Thrift Server trên server `172.17.80.21` tại `queue 1`

    start-thriftserver.sh --master yarn --num-executors 1  --driver-memory 512m --executor-memory 512m  --executor-cores 1 --driver-cores 1 --queue queue1  --hiveconf hive.server2.thrift.port=10015
Câu lệnh kết nối bằng `beeline` với khi yêu cầu `user` `password` ấn `enter` 
    
    beeline
    !connect jdbc:hive2://0.0.0.0:10015/
Để thoát khỏi `beeline` nhập 
    
    !q
Câu lệnh tạo `hive table` từ `hdfs` để truy vấn với `superset`

    create external table spotify_final (title STRING, rank STRING, date STRING, artist STRING, url STRING ,region STRING, chart STRING , trend STRING, streams STRING ) STORED AS PARQUET LOCATION 'hdfs://172.17.80.21:9000/user/tiencm8/btl/output';

Trên server `172.17.80.21` (master) user `hadoop` folder `/home/hadoop/anhlq36/btl` câu lệnh spark-submit:

    spark-submit --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --class Spark.streaming ETL_pipeline_spotify-1.0-SNAPSHOT.jar
