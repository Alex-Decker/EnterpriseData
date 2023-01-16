package org.example.etablishment.beans;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class EtablishmentStat {
    private String codePostal;
    private long nobreEtablishment;
}
