package org.example.Functions;

import org.apache.commons.lang3.StringUtils;
import org.example.Beans.Violation;

import java.io.Serializable;
import java.util.function.Function;

public class TextToViolation implements Function<String, Violation>, Serializable {
    @Override
    public Violation apply(String line) {
        String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, ";");
        String DateReceptionNotification = strings[0];
        String SecteurActivitOrganismeConcern = strings[1];
        String NaturesViolation = strings[2];
        String NombrePersonnesImpactees = strings[3];
        String TypologieDonneesImpactees = strings[4];
        String DonneesSensibles = strings[5];
        String OrigineIncident = strings[6];
        String CausesIncident = strings[7];
        String InformationPersonnes = strings[8];
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
