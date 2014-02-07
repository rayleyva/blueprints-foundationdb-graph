package com.tinkerpop.blueprints.impls.foundationdb;

import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.foundationdb.util.KeyBuilder;
import com.tinkerpop.blueprints.impls.foundationdb.util.Namespace;

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
        Transaction tr = g.getTransaction();
        return new String(tr.get(new KeyBuilder(g).add(Namespace.EDGE).add(this).build()).get());
	}

	@Override
	public Vertex getVertex(Direction direction) throws IllegalArgumentException {
		if (direction.equals(Direction.IN) || direction.equals((Direction.OUT))) {
            Transaction tr = g.getTransaction();
            String vertexID = new String(tr.get(KeyBuilder.directionKeyPrefix(g, direction, this).build()).get());
            return new FoundationDBVertex(g, vertexID);
        }
        else throw new IllegalArgumentException();
	}

    public Future<Vertex> getVertexAsync(Direction direction) throws IllegalArgumentException {
        if (direction.equals(Direction.IN) || direction.equals((Direction.OUT))) {
            Transaction tr = g.getTransaction();
            String vertexID = new String(tr.get(KeyBuilder.directionKeyPrefix(g, direction, this).build()).get());
            Future<byte[]> f = tr.get(KeyBuilder.directionKeyPrefix(g, direction, this).build());
            Future<Vertex> v = f.map(new Function<byte[], Vertex>() {
                @Override
                public Vertex apply(byte[] bytes) {
                    return new FoundationDBVertex(g, new String(bytes));
                }
            });
            return v;
        }
        else throw new IllegalArgumentException();
    }

    @Override
    public void remove() {
        g.removeEdge(this);
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
        FoundationDBEdge other = (FoundationDBEdge) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

}
