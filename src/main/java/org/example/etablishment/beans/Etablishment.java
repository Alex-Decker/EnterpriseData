package org.example.etablishment.beans;

import lombok.*;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
public class Etablishment implements Serializable {
    private String siret;
    private String siren;
    private String codePostal;
    private String siege;
}
