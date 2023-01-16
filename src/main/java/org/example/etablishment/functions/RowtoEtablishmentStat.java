package org.example.etablishment.functions;

import org.apache.spark.sql.Row;
import org.example.etablishment.beans.EtablishmentStat;

import java.io.Serializable;
import java.util.function.Function;

public class RowtoEtablishmentStat implements Function<Row, EtablishmentStat>, Serializable {
    @Override
    public EtablishmentStat apply(Row row) {
        String codePostal = row.getAs("codePostal");
        String nobreEtablishment = row.getAs("nobreEtablishment");
        return EtablishmentStat.builder()
                .codePostal(codePostal)
                .nobreEtablishment(1L)
                .build();
    }
}