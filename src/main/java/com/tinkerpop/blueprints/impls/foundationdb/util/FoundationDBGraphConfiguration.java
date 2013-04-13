package com.tinkerpop.blueprints.impls.foundationdb.util;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.foundationdb.FoundationDBGraph;
import com.tinkerpop.rexster.config.GraphConfiguration;
import org.apache.commons.configuration.Configuration;

public class FoundationDBGraphConfiguration implements GraphConfiguration {

    public Graph configureGraphInstance(final Configuration config) {
        return new FoundationDBGraph();

    }


}
