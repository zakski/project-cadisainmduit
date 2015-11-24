package com.szadowsz.grainne.bean.data;

/**
 * Mapping of gender values in 1901 census.
 *
 * @author Zakski : 30/07/2015.
 */
public enum Language {

    ENGLISH("English"),
    FRENCH("French"),
    GERMAN("German"),
    GREEK("Greek"),
    IRISH("Irish"),
    LATIN("Latin"),
    SCOTCH("Scotch"),
    WELSH("Welsh");

    public final String text;

    Language(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return this.text;
    }
}
