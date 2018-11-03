package com.szadowsz.naming.people.results;

public enum NamePop {

    UNUSED("unused"),RARE("rare"),UNCOMMON("uncommon"),COMMON("common"), BASIC("basic");

    private final String type;

    NamePop(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }
}
