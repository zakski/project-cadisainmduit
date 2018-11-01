package com.szadowz.naming.stellaris.ships;

public enum ShipType {

    COR("corvette"), DES("destroyer"), CRU("cruiser"), BAT("battleship"), TITAN("titan"), COLOS("colossus"), 
    SCI("science"), COLON("colonizer"), CON("constructor"), TRAN("transport"), STAT("military_station_small"), 
    ION("ion_cannon");

    private final String desc;

    ShipType(String desc) {
        this.desc = desc;
    }

    @Override
    public String toString() {
        return desc;
    }
}
