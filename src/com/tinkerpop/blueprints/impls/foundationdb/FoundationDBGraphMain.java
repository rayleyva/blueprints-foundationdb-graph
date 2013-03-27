package com.tinkerpop.blueprints.impls.foundationdb;

import com.tinkerpop.blueprints.*;

public class FoundationDBGraphMain {

	public static void main(String[] args) {
	
		FoundationDBGraph g = new FoundationDBGraph();
		Vertex v = g.addVertex("foo");
		g.shutdown();
		
	}
	
}
