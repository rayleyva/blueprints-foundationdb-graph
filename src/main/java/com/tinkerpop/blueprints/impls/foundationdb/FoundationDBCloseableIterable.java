package com.tinkerpop.blueprints.impls.foundationdb;

import com.foundationdb.async.AsyncIterator;
import com.tinkerpop.blueprints.CloseableIterable;

import java.util.Iterator;

public class FoundationDBCloseableIterable<T> implements CloseableIterable<T> {

    private Iterator<T> myIterator;

    public FoundationDBCloseableIterable(AsyncIterator<T> iterator) {
        this.myIterator = iterator;
    }

    public Iterator<T> iterator() {
        return myIterator;
    }

    public void close() {

    }

}
