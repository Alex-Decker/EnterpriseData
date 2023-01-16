package org.example.etablishment.functions;

import org.apache.spark.sql.Row;
import org.example.etablishment.beans.Etablishment;

import java.io.Serializable;
import java.util.function.Function;

public class RowToEtablishment implements Function<Row, Etablishment>, Serializable {
    @Override
    public Etablishment apply(Row row) {
        String siret = row.getAs("siret");
        String siren = row.getAs("siren");
        String codePostal = row.getAs("codePostalEtablissement");
        String siege = row.getAs("etablissementSiege");
        return Etablishment.builder()
                .codePostal(codePostal)
                .siege(siege)
                .siren(siren)
                .siret(siret)
                .build();
    }
}
