package org.example.Reader;

import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.Beans.Violation;
import org.example.Functions.ViolationMapper;
import org.example.Tools.Tools;

import java.util.function.Supplier;

public class CsvReader implements Supplier<Dataset<Violation>> {
    private final Config config = Tools.getConfig();
    private final SparkSession spark = Tools.initSparkSession();
    private final String inputPath = config.getString("app.path.input");
    private final ViolationMapper mapper = new ViolationMapper();
    @Override
    public Dataset<Violation> get() {
        Dataset<Row> dsRow = spark.read().option("delimiter", ";").option("header", "true").csv(inputPath);

        return mapper.apply(dsRow);
    }
}
