package com.tinkerpop.blueprints.impls.foundationdb;

import java.util.*;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.foundationdb.tuple.Tuple;
import com.foundationdb.Database;
import com.foundationdb.Transaction;

public class FoundationDBVertex extends FoundationDBElement implements Vertex {

	public FoundationDBVertex(FoundationDBGraph g, String vID) {
        super(g);
		this.id = vID;
	}
	
	public FoundationDBVertex(FoundationDBGraph g) {
	    super(g);
    }

	@Override
	public Edge addEdge(String arg0, Vertex arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<Edge> getEdges(Direction direction, String... labels) {
		if (direction.equals(Direction.IN)) return getDirectionEdges("in", labels);
        else if (direction.equals(Direction.OUT)) return getDirectionEdges("out", labels);
        else  {
            ArrayList<Edge> inEdges = getDirectionEdges("in", labels);
            ArrayList<Edge> outEdges = getDirectionEdges("out", labels);
            inEdges.addAll(outEdges);
            return inEdges;
        }
	}

	@Override
	public Iterable<Vertex> getVertices(Direction direction, String... labels) {
		Collection<Vertex> vertices;
        if (direction.equals(Direction.IN)) {
            ArrayList<Edge> edges = getDirectionEdges("in", labels);
            vertices = new ArrayList<Vertex>();
            for (Edge e : edges) {
                vertices.add(e.getVertex(Direction.OUT));
            }
        }
        else if (direction.equals(Direction.OUT)) {
            ArrayList<Edge> edges = getDirectionEdges("out", labels);
            vertices = new ArrayList<Vertex>();
            for (Edge e : edges) {
                vertices.add(e.getVertex(Direction.IN));
            }
        }
        else {
            ArrayList<Edge> inEdges = getDirectionEdges("in", labels);
            ArrayList<Edge> outEdges = getDirectionEdges("out", labels);
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

    private ArrayList<Edge> getDirectionEdges(final String direction, final String... labels) {
        ArrayList<Edge> edges = new ArrayList<Edge>();
        Transaction tr = g.db.createTransaction();
        byte[] edgeBytes = tr.get(g.graphPrefix().add(direction).add(this.getId()).pack()).get();
        if (edgeBytes == null) return new ArrayList<Edge>();
        else {
            List<Object> edgeIds = Tuple.fromBytes(edgeBytes).getItems();
            for (Object edgeID : edgeIds) {
                FoundationDBEdge e = new FoundationDBEdge(g, new String((byte[]) edgeID));
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
        }
        return edges;
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
