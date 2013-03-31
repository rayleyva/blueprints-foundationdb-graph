package com.tinkerpop.blueprints.impls.foundationdb.util;

/**
 * Created with IntelliJ IDEA.
 * User: will
 * Date: 3/30/13
 * Time: 5:51 PM
 * To change this template use File | Settings | File Templates.
 */
public enum ElementType {
    VERTEX(1), EDGE(2);

    private ElementType(int value) {
        this.value = value;
    }

    public final int value;
}
