package org.example.etablishment.functions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.etablishment.beans.Etablishment;

import java.util.function.Function;

public class EtablishmentMapper implements Function<Dataset<Row>, Dataset<Etablishment>> {

    private final  RowToEtablishment parser = new RowToEtablishment();

//    private final MapFunction<Row, Etablishment> task = r ->parser.apply();
    private final MapFunction<Row, Etablishment> task = parser::apply;
    @Override
    public Dataset<Etablishment> apply(Dataset<Row> rowDataset) {
        return rowDataset.map(task, Encoders.bean(Etablishment.class));
    }
}
