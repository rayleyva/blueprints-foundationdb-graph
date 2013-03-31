package com.tinkerpop.blueprints.impls.foundationdb;

import java.util.*;

import com.foundationdb.KeyValue;
import com.tinkerpop.blueprints.*;
import com.foundationdb.tuple.Tuple;
import com.foundationdb.Database;
import com.foundationdb.Transaction;
import com.tinkerpop.blueprints.impls.foundationdb.util.ElementType;
import com.tinkerpop.blueprints.impls.foundationdb.util.KeyBuilder;

public class FoundationDBVertex extends FoundationDBElement implements Vertex {
    @Override
    public Class <? extends Element> getAbstractClass() {
        return Vertex.class;
    }

    @Override
    public ElementType getElementType() {
        return ElementType.VERTEX;
    }

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
		if (d.equals(Direction.IN)) return getDirectionEdges(d, labels);
        else if (d.equals(Direction.OUT)) return getDirectionEdges(d, labels);
        else  {
            ArrayList<Edge> inEdges = getDirectionEdges(Direction.IN, labels);
            ArrayList<Edge> outEdges = getDirectionEdges(Direction.OUT, labels);
            inEdges.addAll(outEdges);
            return inEdges;
        }
	}

	@Override
	public Iterable<Vertex> getVertices(Direction d, String... labels) {
		Collection<Vertex> vertices;
        if (d.equals(Direction.IN)) {
            ArrayList<Edge> edges = getDirectionEdges(d, labels);
            vertices = new ArrayList<Vertex>();
            for (Edge e : edges) {
                vertices.add(e.getVertex(Direction.OUT));
            }
        }
        else if (d.equals(Direction.OUT)) {
            ArrayList<Edge> edges = getDirectionEdges(d, labels);
            vertices = new ArrayList<Vertex>();
            for (Edge e : edges) {
                vertices.add(e.getVertex(Direction.IN));
            }
        }
        else {
            ArrayList<Edge> inEdges = getDirectionEdges(Direction.IN, labels);
            ArrayList<Edge> outEdges = getDirectionEdges(Direction.OUT, labels);
            vertices = new ArrayList<Vertex>();
            for (Edge e : inEdges) {
                vertices.add(e.getVertex(Direction.OUT));
            }
            for (Edge e : outEdges) {
                vertices.add(e.getVertex(Direction.IN));
            }
        }
        return vertices;
	}

	@Override
	public VertexQuery query() {
		// TODO Auto-generated method stub
		return null;
	}

    private ArrayList<Edge> getDirectionEdges(final Direction d, final String... labels) {
        ArrayList<Edge> edges = new ArrayList<Edge>();
        Transaction tr = g.db.createTransaction();
        List<KeyValue> edgeKeys = tr.getRangeStartsWith(new KeyBuilder(g).add(d).add(ElementType.VERTEX).add(this.getId()).build()).asList().get();
        for (KeyValue kv : edgeKeys) {
            FoundationDBEdge e = new FoundationDBEdge(g, Tuple.fromBytes(kv.getKey()).getString(5));
            if (labels.length == 0) {
                edges.add(e);
            }
            else if (labels.length == 1) {
                if (e.getLabel().equals(labels[0])) {
                    edges.add(e);
                }
            }
            else {
                if (Arrays.asList(labels).contains(e.getLabel())) {
                    edges.add(e);
                }
            }
        }
        return edges;
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
