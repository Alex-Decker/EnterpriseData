package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.*;
import org.example.etablishment.functions.EtablishmentMapper;
import org.example.etablishment.functions.StatisticsEtablishment;
import org.example.etablishment.reader.Reader;
import org.example.etablishment.writer.Writer;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

public class Main implements Serializable {
    public static void main(String[] args) {
//        Config config = ConfigFactory.load("application.conf");
//        String masterUrl = config.getString("spark_1.spark.master");
//        String appName = config.getString("spark_1.spark.appName");
//        SparkSession spark = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();
//        String inputPath = config.getString("spark_1.path.input");
//        String outputPath = config.getString("spark_1.path.output");
//        Dataset<Row> ds = spark.read().option("delimiter", ",").option("header","true").csv(inputPath);
//
//        Dataset<Row> statds = ds.groupBy("etablissementSiege").agg(count("etablissementSiege").as("etaiege"),sum("codeCommuneEtablissement").as("x_lgtd"));
//
//        statds.write().partitionBy("etablissementSiege").csv(outputPath);

        Reader reader  = new Reader();
        var ds = reader.get();

        EtablishmentMapper mapper = new EtablishmentMapper();
        var dsEtablishment = mapper.apply(ds);

        StatisticsEtablishment lolo = new StatisticsEtablishment();
        var toto = lolo.apply(dsEtablishment);

        toto.printSchema();
        toto.show(5,false);

//        Writer writer = new Writer();
//        writer.accept(dsEtablishment);

    }
}