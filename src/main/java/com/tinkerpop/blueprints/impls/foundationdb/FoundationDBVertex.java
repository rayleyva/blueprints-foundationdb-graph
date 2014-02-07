package com.tinkerpop.blueprints.impls.foundationdb;

import java.util.*;

import com.foundationdb.KeyValue;
import com.foundationdb.Range;
import com.foundationdb.async.AsyncIterable;
import com.foundationdb.async.AsyncUtil;
import com.foundationdb.async.Function;
import com.google.common.base.*;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.*;
import com.foundationdb.tuple.Tuple;
import com.foundationdb.Transaction;
import com.tinkerpop.blueprints.impls.foundationdb.util.KeyBuilder;

import javax.annotation.Nullable;

public class FoundationDBVertex extends FoundationDBElement implements Vertex {
	public FoundationDBVertex(FoundationDBGraph g, String vID) {
        super(g);
		this.id = vID;
	}
	
	public FoundationDBVertex(FoundationDBGraph g) {
	    super(g);
    }

	@Override
	public Edge addEdge(String label, Vertex inVertex) {
	    return g.addEdge(null, this, inVertex, label);
	}

    @Override
    public Iterable<Edge> getEdges(Direction d, String... labels) {
        return getEdges(d, 0, labels);
    }

	public Iterable<Edge> getEdges(Direction d, int limit, String... labels) {
        Iterable<Edge> result;
        if (d.equals(Direction.IN)) result = getDirectionEdges(d, labels);
        else if (d.equals(Direction.OUT)) result = getDirectionEdges(d, labels);
        else  {
            Iterable<Edge> inEdges = getDirectionEdges(Direction.IN, labels);
            Iterable<Edge> outEdges = getDirectionEdges(Direction.OUT, labels);
            result = Iterables.concat(inEdges, outEdges);
        }

        if (limit != 0) result = Iterables.limit(result, limit);

        return result;
	}

	@Override
    public Iterable<Vertex> getVertices(Direction d, String... labels) {
        return getVertices(d, 0, labels);
    }

	public Iterable<Vertex> getVertices(Direction d, int limit, String... labels) {
		List<Vertex> vertices;
        if (d.equals(Direction.IN) || d.equals(Direction.OUT)) {
            Iterable<Edge> edges = getDirectionEdges(d, labels);
            vertices = new ArrayList<Vertex>();
            for (Edge e : edges) {
                vertices.add(e.getVertex(d.opposite()));
            }
        }
        else {
            Iterable<Edge> inEdges = getDirectionEdges(Direction.IN, labels);
            Iterable<Edge> outEdges = getDirectionEdges(Direction.OUT, labels);
            vertices = new ArrayList<Vertex>();
            for (Edge e : inEdges) {
                vertices.add(e.getVertex(Direction.OUT));
            }
            for (Edge e : outEdges) {
                vertices.add(e.getVertex(Direction.IN));
            }
        }

        if (limit != 0) vertices = vertices.subList(0, limit);

        return vertices;
	}

	@Override
	public VertexQuery query() {
		return new FoundationDBVertexQuery(this);
	}

    private Iterable<Edge> getDirectionEdges(final Direction d, final String... labels) {
        Transaction tr = g.getTransaction();
        AsyncIterable<KeyValue> edgeKeys = tr.getRange(Range.startsWith(KeyBuilder.directionKeyPrefix(g, d, this).build()));
        AsyncIterable<Edge> edges = AsyncUtil.mapIterable(edgeKeys, new Function<KeyValue, Edge>() {
            @Override
            public Edge apply(KeyValue kv) {
                return new FoundationDBEdge(g, Tuple.fromBytes(kv.getKey()).getString(5));
            }
        });
        return Iterables.filter(edges, new Predicate<Edge>() {
            @Override
            public boolean apply(@Nullable Edge e) {
                return (labels == null || labels.length == 0 || (labels.length == 1 && e.getLabel().equals(labels[0])) || Arrays.asList(labels).contains(e.getLabel()));
            }
        });
    }

    @Override
    public void remove() {
        g.removeVertex(this);
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		FoundationDBVertex other = (FoundationDBVertex) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
}
