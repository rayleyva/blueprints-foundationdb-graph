package com.tinkerpop.blueprints.impls.foundationdb.util;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.foundationdb.FoundationDBGraph;
import com.tinkerpop.rexster.config.GraphConfiguration;
import com.tinkerpop.rexster.config.GraphConfigurationException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;

// Minimalist rexster.xml for configuring FoundationDB graph
//  <graph>
//      <graph-name>kvexample</graph-name>
//      <graph-type>com.tinkerpop.blueprints.impls.foundationdb.util.FoundationDBGraphConfiguration</graph-type>
//  </graph>


public class FoundationDBGraphConfiguration implements GraphConfiguration {

    public Graph configureGraphInstance(final Configuration config) throws GraphConfigurationException {
        final HierarchicalConfiguration graphSectionConfig = (HierarchicalConfiguration) config;

        try {
            final String graphName = config.getString("graph-name");
            return new FoundationDBGraph(graphName);

        }
        catch (Exception ex) {
            throw new GraphConfigurationException(ex);
        }

    }
}
