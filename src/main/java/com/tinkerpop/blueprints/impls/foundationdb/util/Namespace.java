package com.tinkerpop.blueprints.impls.foundationdb.util;

/**
 * Created with IntelliJ IDEA.
 * User: will
 * Date: 3/31/13
 * Time: 9:38 AM
 * To change this template use File | Settings | File Templates.
 */
public enum Namespace {
    VERTEX(1), EDGE(2), IN(3), OUT(4), BOTH(5);

    private Namespace(int value) {
        this.value = value;
    }

    public final int value;
}
