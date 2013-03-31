package com.tinkerpop.blueprints.impls.foundationdb;

import java.util.*;

import com.foundationdb.KeyValue;
import com.tinkerpop.blueprints.*;
import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.tinkerpop.blueprints.impls.foundationdb.util.*;
import com.tinkerpop.blueprints.util.PropertyFilteredIterable;

public class FoundationDBGraph implements KeyIndexableGraph, IndexableGraph {
	
	public Database db;
	private FDB fdb;
	public String graphName;
	public static final Features FEATURES = new Features();
    private KeyIndexCache keyIndexCache;
    private AutoIndexer autoIndexer;
	
	static {
        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = true;
        FEATURES.supportsEdgeIndex = true;
        FEATURES.ignoresSuppliedIds = false;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsTransactions = false;
        FEATURES.supportsIndices = true;

        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = false;
        FEATURES.supportsUniformListProperty = false;
        FEATURES.supportsMixedListProperty = false;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = false;
        FEATURES.supportsStringProperty = true;

        FEATURES.isWrapper = false;
        FEATURES.isRDFModel = false;
        FEATURES.supportsKeyIndices = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsThreadedTransactions = false;
    }
	
	public FoundationDBGraph() {
		this(UUID.randomUUID().toString());
	}
	
	public FoundationDBGraph(String graphName) {
		this.graphName = graphName;
		this.fdb = FDB.selectAPIVersion(21);
		this.db = fdb.open().get();
        this.keyIndexCache = new KeyIndexCache(this);
        this.autoIndexer = new AutoIndexer(this);
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
        tr.set(new KeyBuilder(this).add(Namespace.EDGE).add(e).build(), label.getBytes());
        tr.set(KeyBuilder.directionKeyPrefix(this, Direction.IN, e).build(), inVertex.getId().toString().getBytes());
        tr.set(KeyBuilder.directionKeyPrefix(this, Direction.OUT, e).build(), outVertex.getId().toString().getBytes());
        tr.set(KeyBuilder.directionKeyPrefix(this, Direction.IN, inVertex).add(e).build(), "".getBytes());
        tr.set(KeyBuilder.directionKeyPrefix(this, Direction.OUT, outVertex).add(e).build(), "".getBytes());
        tr.commit().get();
        return e;
	}

	@Override
	public FoundationDBVertex addVertex(Object id) {
        FoundationDBVertex v;
		if (id != null) v = new FoundationDBVertex(this, id.toString());
        else v = new FoundationDBVertex(this);
		Transaction tr = db.createTransaction();
		tr.set(new KeyBuilder(this).add(Namespace.VERTEX).add(v).build(), v.getId().getBytes());
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
        List<Edge> edges = new ArrayList<Edge>();
        Transaction tr = db.createTransaction();
        List<KeyValue> keyValueList = tr.getRangeStartsWith(new KeyBuilder(this).add(Namespace.EDGE).build()).asList().get();
        for (KeyValue kv: keyValueList) {
            edges.add(new FoundationDBEdge(this, Tuple.fromBytes(kv.getKey()).getString(3)));
        }
        return edges;
	}

	public Iterable<Edge> getEdges(String key, Object value) {
        if (this.hasKeyIndex(key, ElementType.EDGE)) {
            Transaction tr = db.createTransaction();
            List<KeyValue> kvs = tr.getRangeStartsWith(new KeyBuilder(this).add("kid").add(Namespace.EDGE).add(key).addObject(value).build()).asList().get();
            ArrayList<Edge> edges = new ArrayList<Edge>();
            for (KeyValue kv : kvs) {
                edges.add(new FoundationDBEdge(this, Tuple.fromBytes(kv.getKey()).getString(6)));
            }
            return edges;
        }
        else {
            return new PropertyFilteredIterable<Edge>(key, value, this.getEdges());
        }
	}

	@Override
	public FoundationDBVertex getVertex(Object id) {
		if (id == null) throw new IllegalArgumentException();
		FoundationDBVertex v = new FoundationDBVertex(this, id.toString());
		if (this.hasVertex(v)) return v;
		else return null;
	}

    private Boolean hasVertex(Vertex v) {
        Transaction tr = db.createTransaction();
        return tr.get(new KeyBuilder(this).add(Namespace.VERTEX).add(v).build()).get() != null;
    }

    private Boolean hasEdge(Edge e) {
        Transaction tr = db.createTransaction();
        return tr.get(new KeyBuilder(this).add(Namespace.EDGE).add(e).build()).get() != null;
    }

	@Override
	public Iterable<Vertex> getVertices() {
        List<Vertex> vertices = new ArrayList<Vertex>();
        Transaction tr = db.createTransaction();
        List<KeyValue> keyValueList = tr.getRangeStartsWith(new KeyBuilder(this).add(Namespace.VERTEX).build()).asList().get();
        for (KeyValue kv: keyValueList) {
            vertices.add(new FoundationDBVertex(this, new String(kv.getValue())));
        }
        return vertices;
	}

	@Override
	public Iterable<Vertex> getVertices(String key, Object value) {
        if (this.hasKeyIndex(key, ElementType.VERTEX)) {
            Transaction tr = db.createTransaction();
            List<KeyValue> kvs = tr.getRangeStartsWith(new KeyBuilder(this).add("kid").add(Namespace.VERTEX).add(key).addObject(value).build()).asList().get();
            ArrayList<Vertex> vertices = new ArrayList<Vertex>();
            for (KeyValue kv : kvs) {
                vertices.add(new FoundationDBVertex(this, Tuple.fromBytes(kv.getKey()).getString(6)));
            }
            return vertices;
        }
        else {
            return new PropertyFilteredIterable<Vertex>(key, value, this.getVertices());
        }
	}

	@Override
	public GraphQuery query() {
		return null;
	}

	@Override
	public void removeEdge(Edge e) {
        if (!hasEdge(e)) throw new IllegalArgumentException("Edge does not exist!");
        Transaction tr = db.createTransaction();
        tr.clear(KeyBuilder.directionKeyPrefix(this, Direction.IN, e.getVertex(Direction.IN)).add(e).build());
        tr.clear(KeyBuilder.directionKeyPrefix(this, Direction.OUT, e.getVertex(Direction.OUT)).add(e).build());
        tr.clearRangeStartsWith(new KeyBuilder(this).add(Namespace.EDGE).add(e).build());
        tr.clearRangeStartsWith(KeyBuilder.directionKeyPrefix(this, Direction.IN, e).build());
        tr.clearRangeStartsWith(KeyBuilder.directionKeyPrefix(this, Direction.OUT, e).build());
        tr.clearRangeStartsWith(KeyBuilder.propertyKeyPrefix(this, e).build());
        autoIndexer.autoRemove(e, tr);
        byte[] reverseIndexKey = new KeyBuilder(this).add(Namespace.REVERSE_INDEX).add(Namespace.EDGE).add(e).build();
        List<KeyValue> reverseIndexValues = tr.getRangeStartsWith(reverseIndexKey).asList().get();
        for (KeyValue kv : reverseIndexValues) {
            FoundationDBIndex<Edge> index = new FoundationDBIndex<Edge>(Tuple.fromBytes(kv.getKey()).getString(5), Edge.class, this);
            index.remove(Tuple.fromBytes(kv.getKey()).getString(6), Tuple.fromBytes(kv.getKey()).get(7), e);
        }
        tr.clearRangeStartsWith(reverseIndexKey);
        tr.commit().get();
	}

	@Override
	public void removeVertex(Vertex v) {
        if (!hasVertex(v)) throw new IllegalArgumentException("Vertex does not exist!");
		for (Edge e : v.getEdges(Direction.BOTH)) {
            if (hasEdge(e)) this.removeEdge(e);
        }
        Transaction tr = db.createTransaction();
        tr.clearRangeStartsWith(new KeyBuilder(this).add(Namespace.VERTEX).add(v).build());
        tr.clearRangeStartsWith(KeyBuilder.directionKeyPrefix(this, Direction.IN, v).build());
        tr.clearRangeStartsWith(KeyBuilder.directionKeyPrefix(this, Direction.OUT, v).build());
        tr.clearRangeStartsWith(KeyBuilder.propertyKeyPrefix(this, v).build());
        autoIndexer.autoRemove(v, tr);
        byte[] reverseIndexKey = new KeyBuilder(this).add(Namespace.REVERSE_INDEX).add(Namespace.VERTEX).add(v).build();
        List<KeyValue> reverseIndexValues = tr.getRangeStartsWith(reverseIndexKey).asList().get();
        for (KeyValue kv : reverseIndexValues) {
            FoundationDBIndex<Vertex> index = new FoundationDBIndex<Vertex>(Tuple.fromBytes(kv.getKey()).getString(5), Vertex.class, this);
            index.remove(Tuple.fromBytes(kv.getKey()).getString(6), Tuple.fromBytes(kv.getKey()).get(7), v);
        }
        tr.clearRangeStartsWith(reverseIndexKey);
        tr.commit().get();
	}

	@Override
	public String toString() {
		return "foundationdbgraph [graphName=" + graphName + "]";
	}

	public void shutdown() {

	}

    public void purge() {
        Transaction tr = db.createTransaction();
        tr.clearRangeStartsWith(new KeyBuilder(this).build());
        tr.commit().get();
    }

    public <T extends Element> Index<T> createIndex(String name, Class<T> type, Parameter... args) {
        if (this.hasIndex(name, type)) throw new IllegalStateException();
        Transaction tr = db.createTransaction();
        tr.set(new KeyBuilder(this).add(Namespace.INDICES).add(name).build(), type.getSimpleName().getBytes());
        tr.commit().get();
        return new FoundationDBIndex<T>(name, type, this);
    }

    public <T extends Element> Index<T> getIndex(String name, Class<T> type) {
        FoundationDBIndex<T> index = new FoundationDBIndex<T>(name, type, this);
        if (this.hasIndex(name, type)) return index;
        else return null;
    }

    public void dropIndex(String name) {
        Transaction tr = db.createTransaction();
        tr.clearRangeStartsWith(new KeyBuilder(this).add(Namespace.INDICES).add(name).build());   // todo also remove reverse-index entries
        tr.clearRangeStartsWith(new KeyBuilder(this).add(Namespace.INDEX_DATA).add(name).build());
        tr.commit().get();
    }

    public Iterable<Index<? extends Element>> getIndices() {
        List<Index<? extends Element>> indices = new ArrayList<Index<? extends Element>>();
        Transaction tr = db.createTransaction();
        List<KeyValue> kvs= tr.getRangeStartsWith(new KeyBuilder(this).add(Namespace.INDICES).build()).asList().get();
        for (KeyValue kv : kvs) {
            if (new String(kv.getValue()).equals("Vertex")) {
                indices.add(new FoundationDBIndex<Vertex>(Tuple.fromBytes(kv.getKey()).getString(3), Vertex.class, this));
            }
            else if (new String(kv.getValue()).equals("Edge")) {
                indices.add(new FoundationDBIndex<Edge>(Tuple.fromBytes(kv.getKey()).getString(3), Edge.class, this));
            }
        }
        return indices;
    }

    private <T extends Element> boolean hasIndex(String name, Class<T> type) {
        Transaction tr = db.createTransaction();
        byte[] bytes = tr.get(new KeyBuilder(this).add(Namespace.INDICES).add(name).build()).get();
        return (bytes != null && new String(bytes).equals(type.getSimpleName()));
    }

    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        Transaction tr = db.createTransaction();
        tr.clear(new KeyBuilder(this).add("ki").add(elementClass).add(key).build());
        tr.clearRangeStartsWith(new KeyBuilder(this).add("kid").add(elementClass).add(key).build());
        tr.commit().get();
        keyIndexCache.removeIndexFromCache(key, elementClass);
    }


    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, final Parameter... indexParameters) {
        if (this.hasKeyIndex(key, FoundationDBGraphUtils.getElementType(elementClass))) throw new IllegalArgumentException();
        Transaction tr = db.createTransaction();
        tr.set(new KeyBuilder(this).add("ki").add(elementClass).add(key).build(), "".getBytes());
        autoIndexer.reindexElements(key, elementClass, tr);
        keyIndexCache.addIndexToCache(key, elementClass);
        tr.commit().get();
    }

    public final <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        return keyIndexCache.getIndexedKeys(elementClass);
    }

    public boolean hasKeyIndex(String key, ElementType type) {
        return keyIndexCache.hasKeyIndex(key, type);
    }

    public AutoIndexer getAutoIndexer() {
        return this.autoIndexer;
    }
}
