package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.Processor.ViolationStreamProcessor;
import org.example.Reciver.KafkaReciver;

import java.io.IOException;
import java.util.List;

@Slf4j
public class KafkaStreamingMain {
    public static void main(String[] args) throws InterruptedException, IOException {
        log.info("Hello world!");

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.appName");

        String inputPathStr = config.getString("app.path.input");
        String outputPathStr = config.getString("app.path.output");
        String checkPointStr = config.getString("app.path.checkpoint");
        List<String> topics = config.getStringList("app.kafka.topics");
//        String boostrap_servers = config.getString("app.kafka.boostrap.servers");
//        String group_id = config.getString("app.kafka.group.id");
//        String auto_offset_reset = config.getString("app.auto.offset.reset");

        log.info("\ninputPathStr={}\noutputPathStr={}\ncheckPointStr={}", inputPathStr, outputPathStr, checkPointStr);

        SparkConf sparkConf = new SparkConf().setMaster(masterUrl).setAppName(appName);
        log.info("\nmasterUrl={}\nappName={}", masterUrl, appName);

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        log.info("sparkSession got from sparkConf in the main");

        FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
        log.info("fileSystem got from sparkSession in the main : hdfs.getScheme = {}", hdfs.getScheme());

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(
                checkPointStr,
                () -> {
                    log.info("creating a new javaStreamingContext ...");
                    JavaStreamingContext javaStreamingContext = new JavaStreamingContext(
                            JavaSparkContext.fromSparkContext(sparkSession.sparkContext()),
                            new Duration(1000 * 10)
                    );
                    javaStreamingContext.checkpoint(checkPointStr);

                    KafkaReciver receiver = new KafkaReciver(topics, javaStreamingContext);
                    ViolationStreamProcessor streamProcessor = new ViolationStreamProcessor(outputPathStr);

                    receiver.get().foreachRDD((VoidFunction2<JavaRDD<String>, Time>) streamProcessor);
                    return javaStreamingContext;
                },
                sparkSession.sparkContext().hadoopConfiguration()
        );

        jsc.start();
        jsc.awaitTermination();
    }
}
