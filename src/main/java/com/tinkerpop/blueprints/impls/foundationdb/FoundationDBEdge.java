package com.tinkerpop.blueprints.impls.foundationdb;

import java.util.Set;

import com.foundationdb.Transaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class FoundationDBEdge extends FoundationDBElement implements Edge {

    public FoundationDBEdge(FoundationDBGraph g) {
        super(g);
    }

    public FoundationDBEdge(FoundationDBGraph g, String id) {
        super(g);
        this.id = id;
    }

	@Override
	public String getLabel() {
        Transaction tr = g.db.createTransaction();
        return new String(tr.get(g.graphPrefix().add("e").add(this.getId()).pack()).get());
	}

	@Override
	public Vertex getVertex(Direction direction) throws IllegalArgumentException {
		if (direction.equals(Direction.IN)) {
            Transaction tr = g.db.createTransaction();
            String vertexID = new String(tr.get(g.graphPrefix().add("in").add(this.getId()).pack()).get());
            return new FoundationDBVertex(g, vertexID);
        }
        else if (direction.equals(Direction.OUT)) {
            Transaction tr = g.db.createTransaction();
            String vertexID = new String(tr.get(g.graphPrefix().add("out").add(this.getId()).pack()).get());
            return new FoundationDBVertex(g, vertexID);
        }
        else throw new IllegalArgumentException();
	}

}
