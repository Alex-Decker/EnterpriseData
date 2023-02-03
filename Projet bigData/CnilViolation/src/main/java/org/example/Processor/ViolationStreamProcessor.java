package org.example.Processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.Beans.Violation;
import org.example.Writer.Writer;

@Slf4j
@RequiredArgsConstructor
public class ViolationStreamProcessor implements VoidFunction<JavaRDD<Violation>> {
    private final String outputPathStr;
    @Override
    public void call(JavaRDD<Violation> violationJavaRDD) throws Exception {
        long ts = System.currentTimeMillis();

        log.info("micro-batch stored in folder={}", ts);

        if (violationJavaRDD.isEmpty()) {
            log.info("no data found!");
            return;
        }

        log.info("data under processing...");
        final SparkSession sparkSession = SparkSession.active();


        Dataset<Violation> acteDecesDataset = sparkSession.createDataset(
                violationJavaRDD.rdd(),
                Encoders.bean(Violation.class)
        );

        acteDecesDataset.printSchema();
        acteDecesDataset.show(5, false);

        log.info("nb actesDeces = {}", acteDecesDataset.count());


        Writer<Violation> writer = new Writer<>(outputPathStr + "/time=" + ts,"csv");
        writer.accept(acteDecesDataset);

        acteDecesDataset.unpersist();
        log.info("done");
    }
}
