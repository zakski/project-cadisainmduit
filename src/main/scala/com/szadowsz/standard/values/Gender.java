package com.szadowsz.standard.values;

/**
 * Mapping of gender values in 1901 census.
 *
 * @author Zakski : 30/07/2015.
 */
public enum Gender {

    MALE, FEMALE, UNKNOWN;

    @Override
    public String toString() {
        switch (this) {
            case MALE:
                return "MALE";
            case FEMALE:
                return "FEMALE";
            default:
                return "UNKNOWN";
        }
    }
}
