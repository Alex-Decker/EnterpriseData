package org.example;

import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.example.Beans.Violation;
import org.example.Reader.CsvReader;
import org.example.Tools.Tools;
import org.example.Writer.Writer;

public class Main {
    private static final Config config = Tools.getConfig();

    public static void main(String[] args) {
        CsvReader reader = new CsvReader();
        Dataset<Violation> violationDataset = reader.get();
        violationDataset.show();

//        String outputPath = config.getString("app.path.output");
//        Writer writer = new Writer("json",outputPath);
//
//        writer.accept(violationDataset);
    }
}