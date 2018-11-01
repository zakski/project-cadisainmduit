package com.szadowz.naming.stellaris.planets;

public enum PlanType {

    GENERIC("generic"), DESERT("pc_desert"), TROPICAL("pc_tropical"), ARID("pc_arid"), CONTINENTAL("pc_continental"),
    GAIA("pc_gaia"), OCEAN("pc_ocean"), TUNDRA("pc_tundra"), ARCTIC("pc_arctic"), SAVANNAH("pc_savannah"),
    ALPINE("pc_alpine");

    private final String desc;

    PlanType(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return desc;
    }
}
