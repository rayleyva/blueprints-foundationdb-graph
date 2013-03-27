package com.tinkerpop.blueprints.impls.foundationdb;

import java.util.ArrayList;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;

public class FoundationDBGraph implements Graph {
	
	public Database db;
	private FDB fdb;
	protected String graphName;
	public static final Features FEATURES = new Features();
	
	static {
        FEATURES.supportsDuplicateEdges = false;
        FEATURES.supportsSelfLoops = false;
        FEATURES.isPersistent = false;
        FEATURES.supportsVertexIteration = false;
        FEATURES.supportsEdgeIteration = false;
        FEATURES.supportsVertexIndex = false;
        FEATURES.supportsEdgeIndex = false;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsEdgeRetrieval = false;
        FEATURES.supportsVertexProperties = false;
        FEATURES.supportsEdgeProperties = false;
        FEATURES.supportsTransactions = false;
        FEATURES.supportsIndices = false;

        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = false;
        FEATURES.supportsDoubleProperty = false;
        FEATURES.supportsFloatProperty = false;
        FEATURES.supportsIntegerProperty = false;
        FEATURES.supportsPrimitiveArrayProperty = false;
        FEATURES.supportsUniformListProperty = false;
        FEATURES.supportsMixedListProperty = false;
        FEATURES.supportsLongProperty = false;
        FEATURES.supportsMapProperty = false;
        FEATURES.supportsStringProperty = false;

        FEATURES.isWrapper = false;
        FEATURES.isRDFModel = false;
        FEATURES.supportsKeyIndices = false;
        FEATURES.supportsVertexKeyIndex = false;
        FEATURES.supportsEdgeKeyIndex = false;
        FEATURES.supportsThreadedTransactions = false;
    }
	
	public FoundationDBGraph() {
		new FoundationDBGraph("myGraph");
	}
	
	public FoundationDBGraph(String graphName) {
		this.graphName = graphName;
		this.fdb = FDB.selectAPIVersion(21);
		this.db = fdb.open().get();
	}
	
	public Features getFeatures() {
		return FEATURES;
	}

	@Override
	public Edge addEdge(Object arg0, Vertex arg1, Vertex arg2, String arg3) {
		// TODO Auto-generated method stub
		return new FoundationDBEdge();
	}

	@Override
	public Vertex addVertex(Object arg0) {
		Vertex v = new FoundationDBVertex(db);
		Transaction tr = db.createTransaction();
		Tuple t = new Tuple();
		t.add("/v/").add(arg0.toString());
		tr.set(t.pack(), "1".getBytes());
		tr.commit();
		return v;
	}

	@Override
	public Edge getEdge(Object arg0) {
		// TODO Auto-generated method stub
		return new FoundationDBEdge();
	}

	@Override
	public Iterable<Edge> getEdges() {
		// TODO Auto-generated method stub
		return new ArrayList<Edge>();
	}

	public Iterable<Edge> getEdges(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return new ArrayList<Edge>();
	}

	@Override
	public Vertex getVertex(Object id) {
		// TODO Auto-generated method stub
//		Transaction tr = db.createTransaction();
//		tr.get(id.toString().getBytes())
		if (id == null) throw new IllegalArgumentException();
		FoundationDBVertex v = new FoundationDBVertex(db, id);
		if (v.exists()) return v;
		else return null;
	}

	@Override
	public Iterable<Vertex> getVertices() {
		// TODO Auto-generated method stub
		return new ArrayList<Vertex>();
	}

	@Override
	public Iterable<Vertex> getVertices(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		return new ArrayList<Vertex>();
	}

	@Override
	public GraphQuery query() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void removeEdge(Edge arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeVertex(Vertex arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toString() {
		return "foundationdbgraph [graphName=" + graphName + "]";
	}

	public void shutdown() {
		Transaction tr = db.createTransaction();
		byte[] zero = new byte[1];
		zero[0] = 0;
		tr.clearRangeStartsWith(zero);
		tr.commit();
	}
	


}
