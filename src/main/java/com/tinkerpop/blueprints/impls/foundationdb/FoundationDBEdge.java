package com.tinkerpop.blueprints.impls.foundationdb;

import com.foundationdb.Transaction;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.foundationdb.util.ElementType;
import com.tinkerpop.blueprints.impls.foundationdb.util.KeyBuilder;

public class FoundationDBEdge extends FoundationDBElement implements Edge {
    @Override
    public Class <? extends Element> getAbstractClass() {
        return Edge.class;
    }

    @Override
    public ElementType getElementType() {
        return ElementType.EDGE;
    }

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
        return new String(tr.get(new KeyBuilder(g).add(ElementType.EDGE).add(this).build()).get());
	}

	@Override
	public Vertex getVertex(Direction direction) throws IllegalArgumentException {
		if (direction.equals(Direction.IN) || direction.equals((Direction.OUT))) {
            Transaction tr = g.db.createTransaction();
            String vertexID = new String(tr.get(new KeyBuilder(g).add(direction).add(ElementType.EDGE).add(this).build()).get());
            return new FoundationDBVertex(g, vertexID);
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
