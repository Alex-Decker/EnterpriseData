package org.example.Writer;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.example.Beans.Violation;

import java.util.function.Consumer;
@AllArgsConstructor
public class Writer<T> implements Consumer<Dataset<T>> {
    private final String dataFormat;
    private final String outputPath;
    @Override
    public void accept(Dataset<T> violationDataset) {
        violationDataset
                .write()
                .format(dataFormat)
                .mode("overwrite")
                .option("delimiter",";")
                .save(outputPath);
    }
}
