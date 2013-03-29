package com.tinkerpop.blueprints.impls.foundationdb;

import com.tinkerpop.blueprints.CloseableIterable;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: will
 * Date: 3/28/13
 * Time: 5:28 PM
 * To change this template use File | Settings | File Templates.
 */
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
