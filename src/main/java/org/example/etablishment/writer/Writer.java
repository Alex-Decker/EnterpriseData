package org.example.etablishment.writer;

import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.etablishment.beans.Etablishment;
import org.example.etablishment.reader.Reader;
import tools.Tools;

import java.util.function.Consumer;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

public class Writer implements Consumer<Dataset<Etablishment>> {
    private final Config config = Tools.getConf();
    private final String outputPath = config.getString("spark_1.path.output");


    @Override
    public void accept(Dataset<Etablishment> rowDataset) {


        Dataset<Row> statds = rowDataset.groupBy("codePostal").agg(count("codePostal").as("etSrg"),sum("siret").as("srt"));
        statds.write().csv(outputPath);
    }
}
