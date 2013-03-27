package com.tinkerpop.blueprints.impls.foundationdb;

import com.tinkerpop.blueprints.*;

public class FoundationDBGraphMain {

	public static void main(String[] args) {
		FoundationDBGraph g = new FoundationDBGraph();
		FoundationDBVertex v = g.addVertex(null);
		System.out.println(v.getId().toString());
		System.out.println(g.getVertex(v.getId()).equals(v));
		System.out.println(v.equals(g.getVertex("bar")));
		g.shutdown();		
	}
	
}
