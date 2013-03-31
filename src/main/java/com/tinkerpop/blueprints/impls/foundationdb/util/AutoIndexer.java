package com.tinkerpop.blueprints.impls.foundationdb.util;

import com.foundationdb.Transaction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.foundationdb.FoundationDBGraph;

/**
 * Created with IntelliJ IDEA.
 * User: will
 * Date: 3/31/13
 * Time: 1:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class AutoIndexer {

    private FoundationDBGraph graph;

    public AutoIndexer(FoundationDBGraph g) {
        this.graph = g;
    }

    public void reindexElements(String key, Class<? extends Element> elementClass, Transaction tr) {
        Iterable<Element> elements;
        if (elementClass.equals(Vertex.class)) {
            Iterable<Vertex> vertices = graph.getVertices();
            for (Vertex v : vertices) {
                if (v.getPropertyKeys().contains(key)) {
                    tr.set(new KeyBuilder(graph).add("kid").add(Namespace.VERTEX).add(key).addObject(v.getProperty(key)).add(v).build(), "".getBytes());
                }
            }
            tr.commit().get();
        }
        else if (elementClass.equals(Edge.class)) {
            Iterable<Edge> edges = graph.getEdges();
            for (Edge e : edges) {
                if (e.getPropertyKeys().contains(key)) {
                    tr.set(new KeyBuilder(graph).add("kid").add(Namespace.EDGE).add(key).addObject(e.getProperty(key)).add(e).build(), "".getBytes());
                }
            }
            tr.commit().get();
        }
        else throw new IllegalArgumentException();
    }

    public void autoRemove(Element e, Transaction tr) {
        for (String key : e.getPropertyKeys()) {
            if (graph.hasKeyIndex(key, FoundationDBGraphUtils.getElementType(e))) {
                tr.clear(new KeyBuilder(graph).add("kid").add(FoundationDBGraphUtils.getElementTypeCode(e)).add(key).addObject(e.getProperty(key)).add(e).build());
            }
        }
    }

    public void autoRemove(Element e, String key, Object value, Transaction tr) {
        if (graph.hasKeyIndex(key, FoundationDBGraphUtils.getElementType(e))) {
            tr.clear(new KeyBuilder(graph).add("kid").add(FoundationDBGraphUtils.getElementTypeCode(e)).add(key).addObject(value).add(e).build());
        }
    }

    public void autoAdd(Element e, String key, Object value, Transaction tr) {
        if (graph.hasKeyIndex(key, FoundationDBGraphUtils.getElementType(e))) {
            tr.set(new KeyBuilder(graph).add("kid").add(FoundationDBGraphUtils.getElementTypeCode(e)).add(key).addObject(value).add(e).build(), "".getBytes());
        }
    }

}
