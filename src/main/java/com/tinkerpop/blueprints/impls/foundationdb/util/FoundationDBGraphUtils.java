package com.tinkerpop.blueprints.impls.foundationdb.util;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

public class FoundationDBGraphUtils {

    public static Object getStoreableValue(Object value) {
        if (value instanceof String) {
            return value;
        }
        else if (value instanceof Integer) {
            return value;
        }
        else if (value instanceof Long) {
            return value;
        }
        else if (value instanceof Double) {
            return value.toString();
        }
        else if (value instanceof Float) {
            return value.toString();
        }
        else if (value instanceof Boolean) {
            if (value == Boolean.TRUE) return 1;
            else return 0;
        }
        else throw new IllegalArgumentException();
    }

    public static String getValueTypeString(Object value) {
        if (value instanceof String) {
            return "string";
        }
        else if (value instanceof Integer) {
            return "integer";
        }
        else if (value instanceof Long) {
            return "long";
        }
        else if (value instanceof Double) {
            return "double";
        }
        else if (value instanceof Float) {
            return "float";
        }
        else if (value instanceof Boolean) {
            return "boolean";
        }
        else throw new IllegalArgumentException();
    }

    public static ElementType getElementType(Element e) {
        if (e instanceof Vertex) return ElementType.VERTEX;
        else if (e instanceof Edge) return ElementType.EDGE;
        else throw new IllegalStateException();
    }

    public static ElementType getElementType(Class<? extends Element> elementClass) {
        if (elementClass.equals(Vertex.class)) return ElementType.VERTEX;
        else if (elementClass.equals(Edge.class)) return ElementType.EDGE;
        else throw new IllegalStateException();
    }

    public static int getElementTypeCode(Element e) {
        if (e instanceof Vertex) return Namespace.VERTEX.value;
        else if (e instanceof Edge) return Namespace.EDGE.value;
        else throw new IllegalStateException();
    }

    public static int getElementTypeCode(ElementType t) {
        switch (t) {
            case VERTEX: return Namespace.VERTEX.value;
            case EDGE: return Namespace.EDGE.value;
        }
        throw new IllegalStateException();
    }

    public static int getElementTypeCode(Class<? extends Element> elementClass) {
        if (elementClass.equals(Vertex.class)) return Namespace.VERTEX.value;
        else if (elementClass.equals(Edge.class)) return Namespace.EDGE.value;
        else throw new IllegalStateException();
    }

    public static int getDirectionCode(Direction d) {
        switch (d) {
            case IN: return Namespace.IN.value;
            case OUT: return Namespace.OUT.value;
            case BOTH: return Namespace.BOTH.value;
        }
        throw new IllegalArgumentException();
    }

}