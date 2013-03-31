package com.tinkerpop.blueprints.impls.foundationdb.util;

public enum Namespace {
    VERTEX(1),
    EDGE(2),

    IN(3),
    OUT(4),
    BOTH(5),

    INDICES(6),
    INDEX_DATA(7),
    REVERSE_INDEX(8),

    PROPERTIES(9),

    KEY_INDEX(10),
    KEY_INDEX_DATA(11);

    private Namespace(int value) {
        this.value = value;
    }

    public final int value;
}
