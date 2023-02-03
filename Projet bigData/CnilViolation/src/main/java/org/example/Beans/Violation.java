package org.example.Beans;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class Violation {
    private String DateReceptionNotification;
    private String SecteurActivitOrganismeConcern;
    private String NaturesViolation;
    private String NombrePersonnesImpactees;
    private String TypologieDonneesImpactees;
    private String DonneesSensibles;
    private String OrigineIncident;
    private String CausesIncident;
    private String InformationPersonnes;

}
