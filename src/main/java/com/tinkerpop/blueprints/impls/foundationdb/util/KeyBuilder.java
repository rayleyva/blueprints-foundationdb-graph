package com.tinkerpop.blueprints.impls.foundationdb.util;

import com.foundationdb.tuple.Tuple;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.impls.foundationdb.FoundationDBGraph;

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

    public KeyBuilder add(ElementType type) {
        tuple = tuple.add(FoundationDBGraphUtils.getElementTypeCode(type));
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

    public static KeyBuilder propertyKeyPrefix(FoundationDBGraph g, Element e) {
        return new KeyBuilder(g).add(Namespace.PROPERTIES).add(FoundationDBGraphUtils.getElementTypeCode(e)).add(e);
    }

    public static KeyBuilder keyIndexKeyDataPrefix(FoundationDBGraph g, ElementType type, String key) {
        return new KeyBuilder(g).add(Namespace.KEY_INDEX_DATA).add(type).add(key);
    }

}
