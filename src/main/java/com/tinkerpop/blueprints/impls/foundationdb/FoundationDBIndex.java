package com.tinkerpop.blueprints.impls.foundationdb;

import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.tinkerpop.blueprints.*;
import sun.jvm.hotspot.debugger.win32.coff.DebugVC50TypeLeafIndices;

import java.util.ArrayList;
import java.util.Iterator;

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
        byte[] rawKey = getRawIndexKey(key, value);
        byte[] existingValue = tr.get(rawKey).get();
        if (existingValue == null) {
            tr.set(rawKey, new Tuple().add(element.getId().toString()).pack());
        }
        else {
            Tuple existingValues = Tuple.fromBytes(existingValue);
            tr.set(rawKey, existingValues.add(element.getId().toString()).pack());
        }
        tr.commit().get();
    }


    public CloseableIterable<T> get(String key, Object value) {
        ArrayList<T> items = new ArrayList<T>();
        Transaction tr = g.db.createTransaction();
        byte[] existingValues = tr.get(getRawIndexKey(key, value)).get();
        if (existingValues == null) return new FoundationDBCloseableIterable<T>(items.iterator());
        Tuple tuple = Tuple.fromBytes(existingValues);
        for (Object o : tuple.getItems()) {
            String name = (String) o;
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
        byte[] existingValue = tr.get(getRawIndexKey(key, value)).get();
        if (existingValue == null) return 0;
        Tuple tuple = Tuple.fromBytes(existingValue);
        return tuple.size();
    }


    public void remove(String key, Object value, T element) {
        Transaction tr = g.db.createTransaction();
        tr.clearRangeStartsWith(getRawIndexKey(key, value));
        tr.commit().get();
    }

    private byte[] getRawIndexKey(String key, Object value) {
        return g.graphPrefix().add("iv").add(this.getIndexName()).add(key).addObject(value).pack();
    }
}
