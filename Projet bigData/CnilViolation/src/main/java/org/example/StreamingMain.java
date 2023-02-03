package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.Processor.ViolationStreamProcessor;
import org.example.Reciver.ViolationReciver;

import java.io.IOException;

@Slf4j
public class StreamingMain {
    public static void main(String[] args) throws InterruptedException, IOException {
        log.info("Hello world!");

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.master");
        String appName = config.getString("app.appName");

        String inputPathStr = config.getString("app.path.input");
        String outputPathStr = config.getString("app.path.output");
        String checkPointStr = config.getString("app.path.checkpoint");

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

                    ViolationReciver receiver = new ViolationReciver(javaStreamingContext, inputPathStr);
                    ViolationStreamProcessor streamProcessor = new ViolationStreamProcessor(outputPathStr);

                    receiver.get().foreachRDD(streamProcessor);
                    return javaStreamingContext;
                },
                sparkSession.sparkContext().hadoopConfiguration()
        );

        jsc.start();
        jsc.awaitTermination();
    }
}
