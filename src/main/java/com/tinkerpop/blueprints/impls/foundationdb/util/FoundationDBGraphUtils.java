package com.tinkerpop.blueprints.impls.foundationdb.util;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
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

    public static ElementType getElementType(Class elementClass) {     //todo
        if (elementClass.equals(Vertex.class)) return ElementType.VERTEX;
        else if (elementClass.equals(Edge.class)) return ElementType.EDGE;
        else throw new IllegalStateException();
    }

    public static String getDirectionCode(Direction d) {
        switch (d) {
            case IN: return "in";
            case OUT: return "out";
            case BOTH: return "both";
        }
        throw new IllegalArgumentException();
    }

}