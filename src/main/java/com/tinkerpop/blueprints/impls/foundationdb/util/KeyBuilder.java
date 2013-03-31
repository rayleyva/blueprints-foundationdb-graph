package com.tinkerpop.blueprints.impls.foundationdb.util;

import com.foundationdb.tuple.Tuple;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.foundationdb.FoundationDBElement;
import com.tinkerpop.blueprints.impls.foundationdb.FoundationDBGraph;

/**
 * Created with IntelliJ IDEA.
 * User: will
 * Date: 3/30/13
 * Time: 8:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class KeyBuilder {

    private Tuple tuple;

    public KeyBuilder(FoundationDBGraph g) {
        this.tuple = new Tuple().add(0).add(g.graphName);
    }

    public KeyBuilder add(String string) {
        tuple = tuple.add(string);
        return this;
    }

    public KeyBuilder add(long l) {
        tuple = tuple.add(l);
        return this;
    }

    public KeyBuilder add(Namespace n) {
        tuple = tuple.add(n.value);
        return this;
    }

    public KeyBuilder add(Direction d) {
        tuple = tuple.add(FoundationDBGraphUtils.getDirectionCode(d));
        return this;
    }

    public KeyBuilder addObject(Object value) {
        tuple = tuple.addObject(FoundationDBGraphUtils.getStoreableValue(value));
        return this;
    }

    public KeyBuilder add(Class<? extends Element> elementClass) {
        tuple = tuple.add(FoundationDBGraphUtils.getElementTypeCode(elementClass));
        return this;
    }

    public <T extends Element> KeyBuilder add(T element) {
        tuple = tuple.add(element.getId().toString());
        return this;
    }

    public byte[] build() {
        return tuple.pack();
    }

    public static KeyBuilder directionKeyPrefix(FoundationDBGraph g, Direction d, Element e) {
        return new KeyBuilder(g).add(d).add(FoundationDBGraphUtils.getElementTypeCode(e)).add(e);
    }

}
