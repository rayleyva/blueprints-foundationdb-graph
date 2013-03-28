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
        return (T) new String(bytes);
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
	public <T> T removeProperty(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setProperty(String key, Object value) {
        Transaction tr = g.db.createTransaction();
        tr.set(g.graphPrefix().add("p").add(this.getId()).add(key).pack(), value.toString().getBytes());
        tr.commit();
		
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
