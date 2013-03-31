package com.tinkerpop.blueprints.impls.foundationdb;

import java.util.*;

import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.impls.foundationdb.util.FoundationDBGraphUtils;
import com.tinkerpop.blueprints.impls.foundationdb.util.KeyBuilder;

public abstract class FoundationDBElement implements Element {

	protected String id;
    protected FoundationDBGraph g;

	public FoundationDBElement(FoundationDBGraph g) {
		this.id = UUID.randomUUID().toString();
        this.g = g;
	}
	
	public String getId() {
		return this.id;
	}

	@Override
	public <T> T getProperty(String key) {
		Transaction tr = g.db.createTransaction();
        byte[] bytes = tr.get(this.getRawKey(key)).get();
        if (bytes == null) return null;
        Tuple t = Tuple.fromBytes(bytes);
        String valueType = t.getString(0);
        if (valueType.equals("string")) return (T) t.getString(1);
        else if (valueType.equals("integer")) return (T) new Integer((new Long(t.getLong(1))).intValue());
        else if (valueType.equals("long")) return (T) t.get(1);
        else if (valueType.equals("boolean")) return (T) getBool(t.getLong(1));
        else if (valueType.equals("float")) return (T) new Float(Float.parseFloat(t.getString(1)));
        else if (valueType.equals("double")) return (T) new Double(Double.parseDouble(t.getString(1)));
        else throw new IllegalStateException();
	}

	@Override
	public Set<String> getPropertyKeys() {
        Transaction tr = g.db.createTransaction();
        List<KeyValue> l = tr.getRangeStartsWith(new KeyBuilder(g).add("p").add(getAbstractClass()).add(this).build()).asList().get();
        Set<String> keySet = new TreeSet<String>();
        for (KeyValue kv : l) {
            keySet.add(Tuple.fromBytes(kv.getKey()).getString(5));
        }
        return keySet;
	}

	public void remove() {
		throw new RuntimeException("This needs to be overrided in this implementation");
	}

	@Override
	public <T> T removeProperty(String key) {
        Transaction tr = g.db.createTransaction();
		T value = this.getProperty(key);
        tr.clearRangeStartsWith(this.getRawKey(key));
        if (g.hasKeyIndex(key, this.getAbstractClass())) {
            tr.clear(new KeyBuilder(g).add("kid").add(getAbstractClass()).add(key).addObject(value).add(this).build());
        }
        tr.commit().get();
        return value;
	}

	@Override
	public void setProperty(String key, Object value) {
        if (!(value instanceof String || value instanceof Number || value instanceof Boolean)) throw new IllegalArgumentException();
        if (key.equals("") || key.toLowerCase().equals("id") || key.toLowerCase().equals("label") || key == null) throw new IllegalArgumentException();
        String valueType;
        Transaction tr = g.db.createTransaction();
        if (value instanceof String) {
            valueType = "string";
        }
        else if (value instanceof Integer) {
            valueType = "integer";
        }
        else if (value instanceof Long) {
            valueType = "long";
        }
        else if (value instanceof Double) {
            valueType = "double";
        }
        else if (value instanceof Float) {
            valueType = "float";
        }
        else if (value instanceof Boolean) {
            valueType = "boolean";
        }
        else throw new IllegalArgumentException();
        tr.set(this.getRawKey(key), new Tuple().add(valueType).addObject(FoundationDBGraphUtils.getStoreableValue(value)).pack());
        if (g.hasKeyIndex(key, this.getAbstractClass())) {
            tr.set(new KeyBuilder(g).add("kid").add(getAbstractClass()).add(key).addObject(value).add(this).build(), "".getBytes());
        }
        tr.commit().get();
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FoundationDBElement other = (FoundationDBElement) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

    private Boolean getBool(long l){
        if (l == 0) return Boolean.FALSE;
        if (l == 1) return Boolean.TRUE;
        throw new IllegalArgumentException();
    }

    public abstract Class <? extends Element> getAbstractClass();

    private byte[] getRawKey(String key) {
        return new KeyBuilder(g).add("p").add(getAbstractClass()).add(this).add(key).build();
    }

}
