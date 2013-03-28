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
        FEATURES.ignoresSuppliedIds = false;
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
		this("myGraph");
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
	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
		FoundationDBEdge e;
        if (id != null) e = new FoundationDBEdge(this, id.toString());
        else e = new FoundationDBEdge(this);
        Transaction tr = db.createTransaction();
        tr.set(graphPrefix().add("e").add(e.getId()).pack(), label.getBytes());
        tr.set(graphPrefix().add("in").add(e.getId()).pack(), inVertex.getId().toString().getBytes());
        tr.set(graphPrefix().add("out").add(e.getId()).pack(), outVertex.getId().toString().getBytes());
        byte[] existingInEdgesByte = tr.get(graphPrefix().add("in").add(inVertex.getId().toString()).pack()).get();
        byte[] existingOutEdgesByte = tr.get(graphPrefix().add("out").add(outVertex.getId().toString()).pack()).get();
        Tuple existingInEdges;
        Tuple existingOutEdges;
        if (existingInEdgesByte != null) existingInEdges = Tuple.fromBytes(existingInEdgesByte);
        else existingInEdges = new Tuple();
        if (existingOutEdgesByte != null) existingOutEdges = Tuple.fromBytes(existingOutEdgesByte);
        else existingOutEdges = new Tuple();
        tr.set(graphPrefix().add("in").add(inVertex.getId().toString()).pack(), existingInEdges.add(e.getId().getBytes()).pack());
        tr.set(graphPrefix().add("out").add(outVertex.getId().toString()).pack(), existingOutEdges.add(e.getId().getBytes()).pack());
        tr.commit().get();
        return e;
	}

	@Override
	public FoundationDBVertex addVertex(Object id) {
        FoundationDBVertex v;
		if (id != null) v = new FoundationDBVertex(this, id.toString());
        else v = new FoundationDBVertex(this);
		Transaction tr = db.createTransaction();
		tr.set(graphPrefix().add("v").add(v.getId()).pack(), "1".getBytes());
        tr.commit().get();
		return v;
	}

	@Override
	public Edge getEdge(Object id) {
        if (id == null) throw new IllegalArgumentException();
        FoundationDBEdge e = new FoundationDBEdge(this, id.toString());
        if (this.hasEdge(e)) return e;
        else return null;
	}

	@Override
	public Iterable<Edge> getEdges() {
		throw new UnsupportedOperationException();
	}

	public Iterable<Edge> getEdges(String arg0, Object arg1) {
        throw new UnsupportedOperationException();
	}

	@Override
	public FoundationDBVertex getVertex(Object id) {
		if (id == null) throw new IllegalArgumentException();
		FoundationDBVertex v = new FoundationDBVertex(this, id.toString());
		if (this.hasVertex(v)) return v;
		else return null;
	}

    private Boolean hasVertex(FoundationDBVertex v) {
        Transaction tr = db.createTransaction();
        return tr.get(graphPrefix().add("v").add(v.getId()).pack()).get() != null;
    }

    private Boolean hasEdge(FoundationDBEdge e) {
        Transaction tr = db.createTransaction();
        return tr.get(graphPrefix().add("e").add(e.getId()).pack()).get() != null;
    }

	@Override
	public Iterable<Vertex> getVertices() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterable<Vertex> getVertices(String arg0, Object arg1) {
		throw new UnsupportedOperationException();
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
		tr.clearRangeStartsWith(graphPrefix().pack());
		tr.commit().get();
	}

    public Tuple graphPrefix() {
        return new Tuple().add(0).add(this.graphName);
    }
	


}
