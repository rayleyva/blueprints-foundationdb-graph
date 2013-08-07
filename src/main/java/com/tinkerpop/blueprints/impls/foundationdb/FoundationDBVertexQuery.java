package com.tinkerpop.blueprints.impls.foundationdb;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

public class FoundationDBVertexQuery implements VertexQuery {

    private FoundationDBGraph g;
    private FoundationDBVertex v;
    private Direction d;
    private String[] labels;
    private long limit = 0;

    public FoundationDBVertexQuery(FoundationDBGraph g, FoundationDBVertex v) {
        this.g = g;
        this.v = v;
        this.d = Direction.BOTH;
    }

    public FoundationDBVertexQuery direction(Direction d) {
        this.d = d;
        return this;
    }

    public FoundationDBVertexQuery labels(String[] labels) {
        this.labels = labels;
        return this;
    }

    public FoundationDBVertexQuery has(String key, Object value) {
        return this;
    }

    public <T extends Comparable<T>> FoundationDBVertexQuery has(String key, T value, Compare compare) {
        return this;
    }

    public <T extends Comparable<T>> FoundationDBVertexQuery interval(String key, T value1, T value2) {
        return this;
    }

    public FoundationDBVertexQuery limit(long l) {
        this.limit = l;
        return this;
    }

    public long count() {
        return v.getEdges(d, labels).size();
    }

    public Object vertexIds() {
        return null;
    }

    public Iterable<Vertex> vertices() {
        return v.getVertices(d, (int)limit, labels);
    }

    public Iterable<Edge> edges() {
        return v.getEdges(d, (int)limit, labels);
    }

}
