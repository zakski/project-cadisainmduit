package com.szadowsz.grainne.bean.data;

/**
 * Mapping of gender values in 1901 census.
 *
 * @author Zakski : 30/07/2015.
 */
public enum Gender {

    MALE, FEMALE, MISSING, OTHER;

    @Override
    public String toString() {
        switch (this) {
            case MALE:
                return "MALE";
            case FEMALE:
                return "FEMALE";
            case MISSING:
                return "MISSING";
            default:
                return "OTHER";
        }
    }
}
