package com.tinkerpop.blueprints.impls.foundationdb.util;

import com.foundationdb.Transaction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.foundationdb.FoundationDBGraph;

public class AutoIndexer {

    private FoundationDBGraph graph;

    public AutoIndexer(FoundationDBGraph g) {
        this.graph = g;
    }

    public void reindexElements(String key, Class<? extends Element> elementClass, Transaction tr) {
        if (elementClass.equals(Vertex.class)) {
            Iterable<Vertex> vertices = graph.getVertices();
            for (Vertex v : vertices) {
                if (v.getPropertyKeys().contains(key)) {
                    tr.set(buildKeyIndexKey(v, key, v.getProperty(key)), "".getBytes());
                }
            }
        }
        else if (elementClass.equals(Edge.class)) {
            Iterable<Edge> edges = graph.getEdges();
            for (Edge e : edges) {
                if (e.getPropertyKeys().contains(key)) {
                    tr.set(buildKeyIndexKey(e, key, e.getProperty(key)), "".getBytes());
                }
            }
        }
        else throw new IllegalArgumentException();
    }

    public void autoRemove(Element e, Transaction tr) {
        for (String key : e.getPropertyKeys()) {
            if (graph.hasKeyIndex(key, FoundationDBGraphUtils.getElementType(e))) {
                tr.clear(buildKeyIndexKey(e, key, e.getProperty(key)));
            }
        }
    }

    public void autoRemove(Element e, String key, Object value, Transaction tr) {
        if (graph.hasKeyIndex(key, FoundationDBGraphUtils.getElementType(e))) {
            tr.clear(buildKeyIndexKey(e, key, value));
        }
    }

    public void autoAdd(Element e, String key, Object value, Transaction tr) {
        if (graph.hasKeyIndex(key, FoundationDBGraphUtils.getElementType(e))) {
            tr.set(buildKeyIndexKey(e, key, value), "".getBytes());
        }
    }

    private byte[] buildKeyIndexKey(Element e, String key, Object value) {
        return KeyBuilder.keyIndexKeyDataPrefix(graph, FoundationDBGraphUtils.getElementType(e), key).addObject(value).add(e).build();
    }
}
