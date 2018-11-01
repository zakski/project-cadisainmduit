package com.szadowz.naming.stellaris.armies;

public enum ArmyType {

    GENERIC("generic"), ASSAULT("assault_army"), SLAVE("slave_army"), CLONE("clone_army"), ROBO("robotic_army"),
    ROBO_DEF("robotic_defense_army"), ANDROID("android_army"), ANDROID_DEF("android_defense_army"), PSI("psionic_army"),
    OCCU("occupation_army"), ROBO_OCCU("robotic_occupation_army"), ANDROID_OCCU("android_occupation_army");

    private final String desc;

    ArmyType(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return desc;
    }
}
