package com.tinkerpop.blueprints.impls.foundationdb;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: will
 * Date: 4/13/13
 * Time: 2:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class FoundationDBVertexQuery implements VertexQuery {

    public FoundationDBVertexQuery direction(Direction d) {
        return this;
    }

    public FoundationDBVertexQuery labels(String[] strings) {
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
        return this;
    }

    public long count() {
        return 0;
    }

    public Object vertexIds() {
        return null;
    }

    public Iterable<Vertex> vertices() {
        return new ArrayList<Vertex>();
    }

    public Iterable<Edge> edges() {
        return new ArrayList<Edge>();
    }

}
