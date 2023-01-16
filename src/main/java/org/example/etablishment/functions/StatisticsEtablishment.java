package org.example.etablishment.functions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.example.etablishment.beans.Etablishment;
import org.example.etablishment.beans.EtablishmentStat;

import java.beans.Encoder;
import java.util.function.Function;

import static org.apache.spark.sql.functions.*;

public class StatisticsEtablishment implements Function<Dataset<Etablishment>, Dataset<EtablishmentStat>> {
    private final MapperFunction mf = new MapperFunction();
    @Override
    public Dataset<EtablishmentStat> apply(Dataset<Etablishment> etablishmentDataset) {
//        KeyValueGroupedDataset<String, Etablishment> stringEtablishmentKeyValueGroupedDataset = etablishmentDataset.groupByKey(mf, Encoders.STRING());
//        stringEtablishmentKeyValueGroupedDataset.mapValues();
        //etablishmentDataset.groupByKey(mf, Encoder<String> encoder)
        //return etablishmentDataset.groupBy("codePostal").agg(count("siret").as("nobreEtablishment"));
        //return etablishmentDataset.groupByKey(mf, Encoders.STRING()).agg(count("siret").as("nobreEtablishment"),Encoders.bean(EtablishmentStat.class));
//        return stringEtablishmentKeyValueGroupedDataset.agg(count("siret").as("nobreEtablishment")),Encoders.bean(EtablishmentStat.class);
        //.withColumn("codePostallel",col("codePostal"))
        return null;
    }
}
