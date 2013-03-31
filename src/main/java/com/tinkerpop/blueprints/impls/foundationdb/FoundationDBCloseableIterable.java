package com.tinkerpop.blueprints.impls.foundationdb;

import com.tinkerpop.blueprints.CloseableIterable;

import java.util.Iterator;

public class FoundationDBCloseableIterable<T> implements CloseableIterable<T> {

    private Iterator<T> myIterator;

    public FoundationDBCloseableIterable(Iterator<T> iterator) {
        this.myIterator = iterator;
    }

    public Iterator<T> iterator() {
        return myIterator;
    }

    public void close() {

    }

}
