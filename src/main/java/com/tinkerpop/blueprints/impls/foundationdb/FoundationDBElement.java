package com.tinkerpop.blueprints.impls.foundationdb;

import java.util.Set;
import java.util.UUID;

import com.tinkerpop.blueprints.Element;

public class FoundationDBElement implements Element {

	protected String id;
	
	public FoundationDBElement() {
		this.id = UUID.randomUUID().toString();
	}
	
	public String getId() {
		return this.id;
	}

	@Override
	public <T> T getProperty(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getPropertyKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	public void remove() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public <T> T removeProperty(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setProperty(String arg0, Object arg1) {
		// TODO Auto-generated method stub
		
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
