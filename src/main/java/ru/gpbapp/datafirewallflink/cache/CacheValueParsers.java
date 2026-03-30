package ru.gpbapp.datafirewallflink.cache;

public final class CacheValueParsers {

    private CacheValueParsers() {
    }

    public static String requireString(String value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }
        return value;
    }

    public static Boolean parseBooleanStrict(String value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException(fieldName + " must not be null");
        }

        if ("true".equalsIgnoreCase(value)) {
            return Boolean.TRUE;
        }

        if ("false".equalsIgnoreCase(value)) {
            return Boolean.FALSE;
        }

        throw new IllegalArgumentException(
                fieldName + " must be either 'true' or 'false', but was: " + value
        );
    }
}
