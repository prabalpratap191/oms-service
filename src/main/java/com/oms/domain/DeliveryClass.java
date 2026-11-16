package com.oms.domain;

public enum DeliveryClass {
    INSTANT(15), TWO_DAY(2 * 24 * 60), STANDARD(5 * 24 * 60);

    private final int slaMinutes;
    DeliveryClass(int slaMinutes) { this.slaMinutes = slaMinutes; }
    public int slaMinutes() { return slaMinutes; }

    /** Category constraint: fresh food = 15 min instant, electronics = 2-day courier. */
    public static DeliveryClass forCategory(String category) {
        if (category == null) return STANDARD;
        return switch (category.toUpperCase()) {
            case "FRESH_FOOD", "GROCERY_FRESH" -> INSTANT;
            case "ELECTRONICS" -> TWO_DAY;
            default -> STANDARD;
        };
    }
}
