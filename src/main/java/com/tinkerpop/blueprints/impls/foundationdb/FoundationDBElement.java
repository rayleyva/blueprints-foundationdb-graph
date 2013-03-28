package com.tinkerpop.blueprints.impls.foundationdb;

import java.util.*;

import com.foundationdb.Database;
import com.foundationdb.KeyValue;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.tinkerpop.blueprints.Element;

public class FoundationDBElement implements Element {

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
        byte[] bytes = tr.get(g.graphPrefix().add("p").add(this.getId()).add(key).pack()).get();
        if (bytes == null) return null;
        Tuple t = Tuple.fromBytes(bytes);
        String valueType = t.getString(0);
        if (valueType.equals("string")) return (T) t.getString(1);
        else if (valueType.equals("integer")) return (T) new Integer((new Long(t.getLong(1))).intValue());
        else if (valueType.equals("long")) return (T) t.get(1);
        else if (valueType.equals("boolean")) return (T) t.get(1);
        else throw new IllegalStateException();
	}

	@Override
	public Set<String> getPropertyKeys() {
        Transaction tr = g.db.createTransaction();
        List<KeyValue> l = tr.getRangeStartsWith(g.graphPrefix().add("p").add(this.getId()).pack()).asList().get();
        Set<String> keySet = new TreeSet<String>();
        for (KeyValue kv : l) {
            keySet.add(Tuple.fromBytes(kv.getKey()).getString(4));
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
        tr.clearRangeStartsWith(g.graphPrefix().add("p").add(this.getId()).add(key).pack());
        tr.commit().get();
        return value;
	}

	@Override
	public void setProperty(String key, Object value) {
        if (!(value instanceof String || value instanceof Integer || value instanceof Long || value instanceof Boolean)) throw new IllegalArgumentException();
        if (key.equals("") || key.toLowerCase().equals("id") || key.toLowerCase().equals("label") || key == null) throw new IllegalArgumentException();
        String valueType;
        if (value instanceof String) {
            valueType = "string";
            String rawValue = (String) value;
            Transaction tr = g.db.createTransaction();
            tr.set(g.graphPrefix().add("p").add(this.getId()).add(key).pack(), new Tuple().add(valueType).add(rawValue).pack());
            tr.commit().get();
        }
        else if (value instanceof Integer) {
            valueType = "integer";
            Number rawValue = (Number) value;
            Transaction tr = g.db.createTransaction();
            tr.set(g.graphPrefix().add("p").add(this.getId()).add(key).pack(), new Tuple().add(valueType).addObject(rawValue).pack());
            tr.commit().get();
        }
        else if (value instanceof Long) {
            valueType = "long";
            Number rawValue = (Number) value;
            Transaction tr = g.db.createTransaction();
            tr.set(g.graphPrefix().add("p").add(this.getId()).add(key).pack(), new Tuple().add(valueType).addObject(rawValue).pack());
            tr.commit().get();
        }
        else if (value instanceof Boolean) {
            valueType = "boolean";
            int rawValue;
            if (value == Boolean.TRUE) rawValue = 1;
            else rawValue = 0;
            Transaction tr = g.db.createTransaction();
            tr.set(g.graphPrefix().add("p").add(this.getId()).add(key).pack(), new Tuple().add(valueType).add(rawValue).pack());
            tr.commit().get();
        }
        else throw new IllegalArgumentException();

		
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
	

}
