package com.szadowsz.grainne.data;

/**
 * Created by zakski on 14/11/2015.
 */
public enum County {

    ANTRIM("Antrim"), ARMAGH("Armagh"), CARLOW("Carlow"), CAVAN("Cavan"),
    CLARE("Clare"), CORK("Cork"), DONEGAL("Donegal"), DOWN("Down"),
    DUBLIN("Dublin"), FERMANAGH("Fermanagh"), GALWAY("Galway"), KERRY("Kerry"),
    KILDARE("Kildare"), KILKENNY("Kilkenny"), LAOIS("Laois"), LEITRIM("Leitrim"),
    LIMERICK("Limerick"), DERRY("Derry"), LONGFORD("Longford"), LOUTH("Louth"),
    MAYO("Mayo"), MEATH("Meath"), MONAGHANN("Monaghan"), OFFALY("Offaly"),
    ROSCOMMON("Roscommon"), SLIGO("Sligo"), TIPPERARY("Tipperary"), TYRONE("Tyrone"),
    WATERFORD("Waterford"), WESTMEATH("Westmeath"), WEXFORD("Wexford"), WICKLOW("Wicklow");

    String text;

    County(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return this.text;
    }

    public static County fromString(String text) {
        if (text != null) {
            for (County b : County.values()) {
                if (text.equalsIgnoreCase(b.text)) {
                    return b;
                }
            }
        }
        return null;
    }
}
