package org.example.Functions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.Beans.Violation;

import java.util.function.Function;

public class ViolationMapper implements Function<Dataset<Row>,Dataset<Violation>> {
    private final RowToViolation parser = new RowToViolation();

    private final MapFunction<Row,Violation> task = parser::apply;
    @Override
    public Dataset<Violation> apply(Dataset<Row> rowDataset) {
        return rowDataset.map(task, Encoders.bean(Violation.class));
    }
}
