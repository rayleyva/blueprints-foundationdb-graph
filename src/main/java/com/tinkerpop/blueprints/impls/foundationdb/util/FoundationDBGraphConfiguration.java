package com.tinkerpop.blueprints.impls.foundationdb.util;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.foundationdb.FoundationDBGraph;
import com.tinkerpop.rexster.config.GraphConfiguration;
import com.tinkerpop.rexster.config.GraphConfigurationContext;
import com.tinkerpop.rexster.config.GraphConfigurationException;
import org.apache.commons.configuration.Configuration;

// Minimalist rexster.xml for configuring FoundationDB graph
//  <graph>
//      <graph-name>kvexample</graph-name>
//      <graph-type>com.tinkerpop.blueprints.impls.foundationdb.util.FoundationDBGraphConfiguration</graph-type>
//      <saved-graph-file>demo-graph.xml</saved-graph-file>
//  </graph>


public class FoundationDBGraphConfiguration implements GraphConfiguration {

    public Graph configureGraphInstance(final GraphConfigurationContext context) throws GraphConfigurationException {
        try {
            final Configuration config = context.getProperties();
            final String graphName = config.getString("graph-name");
            final String graphFile = config.getString("saved-graph-file", null);
            if (graphFile == null)
                return new FoundationDBGraph(graphName);
            else
                return new FoundationDBGraph(graphName, graphFile);
        }
        catch (Exception ex) {
            throw new GraphConfigurationException(ex);
        }

    }
}
