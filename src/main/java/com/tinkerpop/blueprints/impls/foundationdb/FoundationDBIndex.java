package com.tinkerpop.blueprints.impls.foundationdb;

import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.foundationdb.util.KeyBuilder;
import com.tinkerpop.blueprints.impls.foundationdb.util.Namespace;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: will
 * Date: 3/28/13
 * Time: 4:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class FoundationDBIndex<T extends Element> implements Index<T> {

    private String name;
    private FoundationDBGraph g;
    private Class<T> indexClass;

    public FoundationDBIndex(String name, Class<T> indexClass, FoundationDBGraph g) {
        this.name = name;
        this.g = g;
        this.indexClass = indexClass;
    }

    public String getIndexName() {
        return this.name;
    }


    public Class<T> getIndexClass() {
        return this.indexClass;
    }


    public void put(String key, Object value, T element) {
        Transaction tr = g.db.createTransaction();
        tr.set(getRawIndexKey(key, value).add(element).build(), "".getBytes());
        tr.set(getRawReverseIndexKey(element, key, value), "".getBytes());
        tr.commit().get();
    }


    public CloseableIterable<T> get(String key, Object value) {
        ArrayList<T> items = new ArrayList<T>();
        Transaction tr = g.db.createTransaction();
        List<KeyValue> existingValues = tr.getRangeStartsWith(getRawIndexKey(key, value).build()).asList().get();
        for (KeyValue kv : existingValues) {
            String name = Tuple.fromBytes(kv.getKey()).getString(6);
            if (this.getIndexClass().equals(Vertex.class)) items.add((T) new FoundationDBVertex(g, name));
            else if (this.getIndexClass().equals(Edge.class)) items.add((T) new FoundationDBEdge(g, name));
        }
        return new FoundationDBCloseableIterable<T>(items.iterator());
    }


    public CloseableIterable<T> query(String key, Object query) {
        throw new UnsupportedOperationException();
    }


    public long count(String key, Object value) {
        Transaction tr = g.db.createTransaction();
        List<KeyValue> existingValues = tr.getRangeStartsWith(getRawIndexKey(key, value).build()).asList().get();
        return existingValues.size();
    }


    public void remove(String key, Object value, T element) {
        Transaction tr = g.db.createTransaction();
        tr.clear(getRawIndexKey(key, value).add(element).build());
        tr.commit().get();
    }

    private KeyBuilder getRawIndexKey(String key, Object value) {          // todo separate keyspaces!
        return new KeyBuilder(g).add(Namespace.INDEX_DATA).add(getIndexName()).add(key).addObject(value);
    }

    private byte[] getRawReverseIndexKey(T e, String key, Object value) {
        return new KeyBuilder(g).add(Namespace.REVERSE_INDEX).add(indexClass).add(e).add(getIndexName()).add(key).addObject(value).build();
    }
}
