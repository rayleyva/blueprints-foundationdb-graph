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
import com.tinkerpop.blueprints.util.io.gml.GMLReader;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReader;

public class FoundationDBGraph implements KeyIndexableGraph, IndexableGraph, TransactionalGraph {
	
	private Database db;
	public String graphName;
	public static final Features FEATURES = new Features();
    private AutoIndexer autoIndexer;
    private ThreadLocal<Transaction> tr;
	
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
        FEATURES.supportsTransactions = true;
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
        FDB fdb = FDB.selectAPIVersion(21);
        this.graphName = graphName;
		this.db = fdb.open().get();
        this.autoIndexer = new AutoIndexer(this);
        this.tr = new ThreadLocal<Transaction>() {
            protected Transaction initialValue() {
                return db.createTransaction();
            }
        };
	}

    public FoundationDBGraph(String graphName, String graphFile) {
        this(graphName);
        if (getTransaction().get(new KeyBuilder(this).build()).get() == null ) {
            System.out.println("Reading file: ".concat(graphFile));
            readGraphFile(graphFile);
            getTransaction().commit();
        }
        else {
            System.out.println("File not loaded! Restoring persisted graph from disk.");
        }
    }

    private void readGraphFile(String graphFile) {
        if (graphFile.endsWith(".gml")) {
            GMLReader gmlReader = new GMLReader(this);
            try {
                gmlReader.inputGraph(graphFile);
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }
        }
        else if (graphFile.endsWith(".xml")) {
            GraphMLReader graphMLReader = new GraphMLReader(this);
            try {
                graphMLReader.inputGraph(graphFile);
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }
        }
        else if (graphFile.endsWith(".json")) {
            GraphSONReader graphSONReader = new GraphSONReader(this);
            try {
                graphSONReader.inputGraph(graphFile);
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }
        }
    }
	
	public Features getFeatures() {
		return FEATURES;
	}

	@Override
	public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
		FoundationDBEdge e;
        if (id != null) e = new FoundationDBEdge(this, id.toString());
        else e = new FoundationDBEdge(this);
        Transaction tr = getTransaction();
        tr.set(new KeyBuilder(this).add(Namespace.EDGE).add(e).build(), label.getBytes());
        tr.set(KeyBuilder.directionKeyPrefix(this, Direction.IN, e).build(), inVertex.getId().toString().getBytes());
        tr.set(KeyBuilder.directionKeyPrefix(this, Direction.OUT, e).build(), outVertex.getId().toString().getBytes());
        tr.set(KeyBuilder.directionKeyPrefix(this, Direction.IN, inVertex).add(e).build(), "".getBytes());
        tr.set(KeyBuilder.directionKeyPrefix(this, Direction.OUT, outVertex).add(e).build(), "".getBytes());
        return e;
	}

	@Override
	public FoundationDBVertex addVertex(Object id) {
        FoundationDBVertex v;
		if (id != null) v = new FoundationDBVertex(this, id.toString());
        else v = new FoundationDBVertex(this);
		Transaction tr = getTransaction();
		tr.set(new KeyBuilder(this).add(Namespace.VERTEX).add(v).build(), v.getId().getBytes());
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
        Transaction tr = getTransaction();
        List<KeyValue> keyValueList = tr.getRangeStartsWith(new KeyBuilder(this).add(Namespace.EDGE).build()).asList().get();
        for (KeyValue kv: keyValueList) {
            edges.add(new FoundationDBEdge(this, Tuple.fromBytes(kv.getKey()).getString(3)));
        }
        return edges;
	}

	public Iterable<Edge> getEdges(String key, Object value) {
        if (this.hasKeyIndex(key, ElementType.EDGE)) {
            Transaction tr = getTransaction();
            List<KeyValue> kvs = tr.getRangeStartsWith(KeyBuilder.keyIndexKeyDataPrefix(this, ElementType.EDGE, key).addObject(value).build()).asList().get();
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
        Transaction tr = getTransaction();
        return tr.get(new KeyBuilder(this).add(Namespace.VERTEX).add(v).build()).get() != null;
    }

    private Boolean hasEdge(Edge e) {
        Transaction tr = getTransaction();
        return tr.get(new KeyBuilder(this).add(Namespace.EDGE).add(e).build()).get() != null;
    }

	@Override
	public Iterable<Vertex> getVertices() {
        List<Vertex> vertices = new ArrayList<Vertex>();
        Transaction tr = getTransaction();
        List<KeyValue> keyValueList = tr.getRangeStartsWith(new KeyBuilder(this).add(Namespace.VERTEX).build()).asList().get();
        for (KeyValue kv: keyValueList) {
            vertices.add(new FoundationDBVertex(this, new String(kv.getValue())));
        }
        return vertices;
	}

	@Override
	public Iterable<Vertex> getVertices(String key, Object value) {
        if (this.hasKeyIndex(key, ElementType.VERTEX)) {
            Transaction tr = getTransaction();
            List<KeyValue> kvs = tr.getRangeStartsWith(KeyBuilder.keyIndexKeyDataPrefix(this, ElementType.VERTEX, key).addObject(value).build()).asList().get();
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
        Transaction tr = getTransaction();
        tr.clear(KeyBuilder.directionKeyPrefix(this, Direction.IN, e.getVertex(Direction.IN)).add(e).build());
        tr.clear(KeyBuilder.directionKeyPrefix(this, Direction.OUT, e.getVertex(Direction.OUT)).add(e).build());
        tr.clearRangeStartsWith(new KeyBuilder(this).add(Namespace.EDGE).add(e).build());
        tr.clearRangeStartsWith(KeyBuilder.directionKeyPrefix(this, Direction.IN, e).build());
        tr.clearRangeStartsWith(KeyBuilder.directionKeyPrefix(this, Direction.OUT, e).build());
        autoIndexer.autoRemove(e, tr);
        tr.clearRangeStartsWith(KeyBuilder.propertyKeyPrefix(this, e).build());
        byte[] reverseIndexKey = new KeyBuilder(this).add(Namespace.REVERSE_INDEX).add(Namespace.EDGE).add(e).build();
        List<KeyValue> reverseIndexValues = tr.getRangeStartsWith(reverseIndexKey).asList().get();
        for (KeyValue kv : reverseIndexValues) {
            FoundationDBIndex<Edge> index = new FoundationDBIndex<Edge>(Tuple.fromBytes(kv.getKey()).getString(5), Edge.class, this);
            index.remove(Tuple.fromBytes(kv.getKey()).getString(6), Tuple.fromBytes(kv.getKey()).get(7), e);
        }
        tr.clearRangeStartsWith(reverseIndexKey);
	}

	@Override
	public void removeVertex(Vertex v) {
        if (!hasVertex(v)) throw new IllegalArgumentException("Vertex does not exist!");
		for (Edge e : v.getEdges(Direction.BOTH)) {
            if (hasEdge(e)) this.removeEdge(e);
        }
        Transaction tr = getTransaction();
        tr.clearRangeStartsWith(new KeyBuilder(this).add(Namespace.VERTEX).add(v).build());
        tr.clearRangeStartsWith(KeyBuilder.directionKeyPrefix(this, Direction.IN, v).build());
        tr.clearRangeStartsWith(KeyBuilder.directionKeyPrefix(this, Direction.OUT, v).build());
        autoIndexer.autoRemove(v, tr);
        tr.clearRangeStartsWith(KeyBuilder.propertyKeyPrefix(this, v).build());
        byte[] reverseIndexKey = new KeyBuilder(this).add(Namespace.REVERSE_INDEX).add(Namespace.VERTEX).add(v).build();
        List<KeyValue> reverseIndexValues = tr.getRangeStartsWith(reverseIndexKey).asList().get();
        for (KeyValue kv : reverseIndexValues) {
            FoundationDBIndex<Vertex> index = new FoundationDBIndex<Vertex>(Tuple.fromBytes(kv.getKey()).getString(5), Vertex.class, this);
            index.remove(Tuple.fromBytes(kv.getKey()).getString(6), Tuple.fromBytes(kv.getKey()).get(7), v);
        }
        tr.clearRangeStartsWith(reverseIndexKey);
	}

	@Override
	public String toString() {
		return "foundationdbgraph [graphName=" + graphName + "]";
	}

	public void shutdown() {
        commit();
	}

    public void purge() {
        Transaction tr = db.createTransaction();
        tr.clearRangeStartsWith(new KeyBuilder(this).build());
        tr.commit().get();
    }

    public <T extends Element> Index<T> createIndex(String name, Class<T> type, Parameter... args) {
        FoundationDBIndex<T> index = new FoundationDBIndex<T>(name, type, this);
        Transaction tr = getTransaction();
        if (index.exists(name, type, tr)) throw new IllegalStateException();
        tr.set(new KeyBuilder(this).add(Namespace.INDICES).add(name).build(), type.getSimpleName().getBytes());
        return new FoundationDBIndex<T>(name, type, this);
    }

    public <T extends Element> Index<T> getIndex(String name, Class<T> type) {
        FoundationDBIndex<T> index = new FoundationDBIndex<T>(name, type, this);
        if (index.exists(name, type, getTransaction())) return index;
        else return null;
    }

    public void dropIndex(String name) {
        Transaction tr = getTransaction();
        tr.clearRangeStartsWith(new KeyBuilder(this).add(Namespace.INDICES).add(name).build());   // todo also remove reverse-index entries
        tr.clearRangeStartsWith(new KeyBuilder(this).add(Namespace.INDEX_DATA).add(name).build());
    }

    public Iterable<Index<? extends Element>> getIndices() {
        List<Index<? extends Element>> indices = new ArrayList<Index<? extends Element>>();
        Transaction tr = getTransaction();
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



    public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
        Transaction tr = getTransaction();
        tr.clear(new KeyBuilder(this).add(Namespace.KEY_INDEX).add(elementClass).add(key).build());
        tr.clearRangeStartsWith(KeyBuilder.keyIndexKeyDataPrefix(this, FoundationDBGraphUtils.getElementType(elementClass), key).build());
    }


    public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, final Parameter... indexParameters) {
        if (this.hasKeyIndex(key, FoundationDBGraphUtils.getElementType(elementClass))) throw new IllegalArgumentException();
        Transaction tr = getTransaction();
        tr.set(new KeyBuilder(this).add(Namespace.KEY_INDEX).add(elementClass).add(key).build(), "".getBytes());
        autoIndexer.reindexElements(key, elementClass, tr);
    }

    public final <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        Set<String> keyIndices = new TreeSet<String>();
        if(elementClass.equals(Vertex.class) || elementClass.equals(Edge.class)) {
            List<KeyValue> kvs = getTransaction().getRangeStartsWith(new KeyBuilder(this).add(Namespace.KEY_INDEX).add(elementClass).build()).asList().get();
            for (KeyValue kv : kvs) {
                keyIndices.add(Tuple.fromBytes(kv.getKey()).getString(4));
            }
            return keyIndices;
        }
        else throw new IllegalArgumentException();
    }

    public boolean hasKeyIndex(String key, ElementType type) {
        return getIndexedKeys(FoundationDBGraphUtils.getElementClass(type)).contains(key);
    }

    public AutoIndexer getAutoIndexer() {
        return this.autoIndexer;
    }

    public void stopTransaction(Conclusion conclusion) {
        if (conclusion == Conclusion.SUCCESS) commit();
        else if (conclusion == Conclusion.FAILURE) rollback();
    }

    public void commit() {
        this.tr.get().commit().get();
        this.tr.remove();
    }

    public void rollback() {
        this.tr.get().reset();
        this.tr.get().dispose();
        this.tr.remove();
    }

    public Transaction getTransaction() {
        return this.tr.get();
    }

}
