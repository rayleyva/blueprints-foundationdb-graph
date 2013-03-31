package com.tinkerpop.blueprints.impls.foundationdb.util;

import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.foundationdb.FoundationDBGraph;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA.
 * User: will
 * Date: 3/31/13
 * Time: 1:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class KeyIndexCache {
    private Set<String> vertexKeyIndices;
    private Set<String> edgeKeyIndices;
    private FoundationDBGraph graph;

    public KeyIndexCache(FoundationDBGraph graph) {
        this.graph = graph;
        this.vertexKeyIndices = new TreeSet<String>();
        Transaction tr = graph.db.createTransaction();
        List<KeyValue> kvs = tr.getRangeStartsWith(new KeyBuilder(graph).add("ki").add(Namespace.VERTEX).build()).asList().get();
        for (KeyValue kv : kvs) {
            this.vertexKeyIndices.add(Tuple.fromBytes(kv.getKey()).getString(4));
        }
        this.edgeKeyIndices = new TreeSet<String>();
        kvs = tr.getRangeStartsWith(new KeyBuilder(graph).add("ki").add(Namespace.EDGE).build()).asList().get();
        for (KeyValue kv : kvs) {
            this.edgeKeyIndices.add(Tuple.fromBytes(kv.getKey()).getString(4));
        }
        tr.commit().get();
    }

    public <T extends Element> void addIndexToCache(String key, Class<T> elementClass) {
        if (elementClass.equals(Vertex.class)) this.vertexKeyIndices.add(key);
        else if (elementClass.equals(Edge.class)) this.edgeKeyIndices.add(key);
        else throw new IllegalStateException();
    }

    public <T extends Element> void removeIndexFromCache(String key, Class<T> elementClass) {
        if (elementClass.equals(Vertex.class)) this.vertexKeyIndices.remove(key);
        else if (elementClass.equals(Edge.class)) this.edgeKeyIndices.remove(key);
        else throw new IllegalStateException();
    }

    public <T extends Element> boolean hasKeyIndex(String key, ElementType type) {
        if (type.equals(ElementType.VERTEX)) return this.vertexKeyIndices.contains(key);
        else if (type.equals(ElementType.EDGE)) return this.edgeKeyIndices.contains(key);
        else throw new IllegalStateException();
    }

    public final <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        if (elementClass.equals(Vertex.class)) return this.vertexKeyIndices;
        else if (elementClass.equals(Edge.class)) return this.edgeKeyIndices;
        else throw new IllegalStateException();
    }
}
