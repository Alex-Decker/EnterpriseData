package org.example.etablishment.functions;

import java.util.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.etablishment.beans.EtablishmentStat;

public class EtablishmentStatMapper implements Function<Dataset<Row>, Dataset<EtablishmentStat>> {

    private final  RowtoEtablishmentStat parser = new RowtoEtablishmentStat();

    //    private final MapFunction<Row, Etablishment> task = r ->parser.apply();
    private final MapFunction<Row, EtablishmentStat> task = parser::apply;
    @Override
    public Dataset<EtablishmentStat> apply(Dataset<Row> rowDataset) {
            return rowDataset.map(task, Encoders.bean(EtablishmentStat.class));
    }
}
