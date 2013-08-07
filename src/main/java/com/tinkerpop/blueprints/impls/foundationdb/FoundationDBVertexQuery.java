package com.tinkerpop.blueprints.impls.foundationdb;

import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

public class FoundationDBVertexQuery extends DefaultVertexQuery implements VertexQuery {

    public FoundationDBVertexQuery(FoundationDBVertex v) {
        super(v);
    }

}
