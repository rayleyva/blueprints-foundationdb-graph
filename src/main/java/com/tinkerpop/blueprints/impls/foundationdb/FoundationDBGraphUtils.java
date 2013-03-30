package com.tinkerpop.blueprints.impls.foundationdb;

public class FoundationDBGraphUtils {

    public static Object getStoreableValue(Object value) {
        if (value instanceof String) {
            return value;
        }
        else if (value instanceof Integer) {
            return value;
        }
        else if (value instanceof Long) {
            return value;
        }
        else if (value instanceof Double) {
            return value.toString();
        }
        else if (value instanceof Float) {
            return value.toString();
        }
        else if (value instanceof Boolean) {
            if (value == Boolean.TRUE) return 1;
            else return 0;
        }
        else throw new IllegalArgumentException();
    }

}