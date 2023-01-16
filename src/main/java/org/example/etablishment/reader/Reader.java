package org.example.etablishment.reader;

import com.esotericsoftware.reflectasm.FieldAccess;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import tools.Tools;

import java.util.function.Supplier;

public class Reader implements Supplier<Dataset<Row>> {

    private final Config config = Tools.getConf();

    private final SparkSession spark = Tools.initSparkSession();
    private final String inputPath = config.getString("spark_1.path.input");
    @Override
    public Dataset<Row> get() {
        return spark.read().option("delimiter", ",").option("header","true").csv(inputPath);

    }
}
