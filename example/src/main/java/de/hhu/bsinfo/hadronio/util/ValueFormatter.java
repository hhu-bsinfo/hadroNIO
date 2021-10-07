package de.hhu.bsinfo.hadronio.util;

public class ValueFormatter {

    private ValueFormatter() {}

    private static final char[] highMetricTable = {
            0,
            'K',
            'M',
            'G',
            'T',
            'P',
            'E'
    };

    private static final char[] lowMetricTable = {
            0,
            'm',
            'u',
            'n',
            'p',
            'f',
            'a'
    };

    private static String formatHighValue(final double value, final String unit) {
        double formattedValue = value;

        int counter = 0;
        while(formattedValue > 1000 && counter < highMetricTable.length - 1) {
            formattedValue /= 1000;
            counter++;
        }

        if(value == (long) value) {
            return String.format("%.3f %c%s (%d)", formattedValue, highMetricTable[counter], unit, (long) value);
        }

        return String.format("%.3f %c%s (%f)", formattedValue, highMetricTable[counter], unit, value);
    }

    private static String formatLowValue(final double value, final String unit) {
        double formattedValue = value;

        int counter = 0;
        while(formattedValue < 1 && formattedValue != 0 && counter < lowMetricTable.length - 1) {
            formattedValue *= 1000;
            counter++;
        }

        if(value == (long) value) {
            return String.format("%.3f %c%s (%d)", formattedValue, lowMetricTable[counter], unit, (long) value);
        }

        return String.format("%.3f %c%s (%f)", formattedValue, lowMetricTable[counter], unit, value);
    }

    public static String formatValue(final double value, final String unit) {
        if(value >= 1) {
            return formatHighValue(value, unit);
        } else {
            return formatLowValue(value, unit);
        }
    }

    public static String formatValue(final String name, final double value, final String unit) {
        return String.format("%-20s %s", name + ":", formatValue(value, unit));
    }

    public static String formatValue(final String name, final double value) {
        return formatValue(name, value, "Units");
    }
}
