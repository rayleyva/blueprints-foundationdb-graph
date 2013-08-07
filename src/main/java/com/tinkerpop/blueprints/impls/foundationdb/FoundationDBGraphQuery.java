package com.tinkerpop.blueprints.impls.foundationdb;

import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;

public class FoundationDBGraphQuery extends DefaultGraphQuery implements GraphQuery {

    public FoundationDBGraphQuery(FoundationDBGraph g) {
        super(g);
    }

}
