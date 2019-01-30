package com.netcompany.techtalk.batch.util;

public final class StringUtils {

    private StringUtils() {}

    public static boolean nullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }

}
