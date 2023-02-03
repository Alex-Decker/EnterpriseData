package org.example.Functions;

import org.apache.spark.sql.Row;
import org.example.Beans.Violation;

import java.io.Serializable;
import java.util.function.Function;

public class RowToViolation implements Function<Row, Violation>, Serializable {
    @Override
    public Violation apply(Row row) {
        String DateReceptionNotification = row.getAs("Date de r�ception de la notification�");
        String SecteurActivitOrganismeConcern = row.getAs("Secteur d'activit� de l'organisme concern�");
        String NaturesViolation = row.getAs("Natures de la violation");
        String NombrePersonnesImpactees = row.getAs("Nombre de personnes impact�es");
        String TypologieDonneesImpactees = row.getAs("Typologie des donn�es impact�es");
        String DonneesSensibles = row.getAs("Donn�es sensibles");
        String OrigineIncident = row.getAs("Origine de l'incident");
        String CausesIncident = row.getAs("Causes de l'incident");
        String InformationPersonnes = row.getAs("Information des personnes");
        return Violation.builder()
                .DateReceptionNotification(DateReceptionNotification)
                .SecteurActivitOrganismeConcern(SecteurActivitOrganismeConcern)
                .NaturesViolation(NaturesViolation)
                .NombrePersonnesImpactees(NombrePersonnesImpactees)
                .TypologieDonneesImpactees(TypologieDonneesImpactees)
                .DonneesSensibles(DonneesSensibles)
                .OrigineIncident(OrigineIncident)
                .CausesIncident(CausesIncident)
                .InformationPersonnes(InformationPersonnes)
                .build();
    }
}
