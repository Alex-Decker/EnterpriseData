package org.example.functions.statistic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.etablishment.beans.Etablishment;
import org.example.etablishment.functions.StatisticsEtablishment;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SatatisticEtablishmentTest {
    @Test
    public void testApply() {
//        SparkSession spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate();
//
//        List<Etablishment> etablishments = Arrays.asList(
//                new Etablishment("siret1", "molo","87000","true"),
//                new Etablishment("siret2", "molo","87000","true"),
//                new Etablishment("siret3", "molo","75000","false")
//        );
//
//        Dataset<Etablishment> etablishmentDataset = spark.createDataset(etablishments, Encoders.bean(Etablishment.class));
//
//        StatisticsEtablishment statisticsEtablishment = new StatisticsEtablishment();
//        Dataset<Row> result = statisticsEtablishment.apply(etablishmentDataset);
//
//        assertEquals(2, result.count());
//        assertEquals(2, result.filter("codePostal = '87000'").first().getLong(1));
//        assertEquals(1, result.filter("codePostal = '75000'").first().getLong(1));
    }

}
