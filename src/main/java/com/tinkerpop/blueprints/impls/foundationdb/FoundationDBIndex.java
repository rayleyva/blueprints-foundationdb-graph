package com.tinkerpop.blueprints.impls.foundationdb;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;

/**
 * Created with IntelliJ IDEA.
 * User: will
 * Date: 3/28/13
 * Time: 4:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class FoundationDBIndex<T extends Element> implements Index<T> {

    public String getIndexName() {
        return null;
    }


    public Class<T> getIndexClass() {
        return null;
    }


    public void put(String key, Object value, T element) {
    }


    public CloseableIterable<T> get(String key, Object value) {
        return null;
    }


    public CloseableIterable<T> query(String key, Object query) {
        return null;
    }


    public long count(String key, Object value) {
        return 0;
    }


    public void remove(String key, Object value, T element) {

    }
}
