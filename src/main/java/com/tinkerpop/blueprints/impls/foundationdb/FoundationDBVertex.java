package com.tinkerpop.blueprints.impls.foundationdb;

import java.util.Set;

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
	public Iterable<Edge> getEdges(Direction arg0, String... arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<Vertex> getVertices(Direction arg0, String... arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VertexQuery query() {
		// TODO Auto-generated method stub
		return null;
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
