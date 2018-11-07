package com.szadowsz.naming.people.results;

import java.util.Arrays;

public enum NamePop {

    UNUSED("unused"),ULTRA_RARE("ultra-rare"),RARE("rare"),UNCOMMON("uncommon"),COMMON("common"), BASIC("basic");

    private final String type;

    NamePop(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }
    
    public static NamePop typeOf(String type){
        return Arrays.stream(NamePop.values()).filter(n -> n.type.equals(type)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No enum constant " + NamePop.class.getName() + "." + type.toUpperCase()));
    }
}
