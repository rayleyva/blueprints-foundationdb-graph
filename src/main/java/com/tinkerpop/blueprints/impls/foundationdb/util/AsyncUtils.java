package com.tinkerpop.blueprints.impls.foundationdb.util;

import com.foundationdb.async.AsyncIterable;
import com.foundationdb.async.AsyncIterator;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;

import java.util.ArrayList;
import java.util.List;

public class AsyncUtils {
    /**
     * Map an {@code AsyncIterable} into an {@code AsyncIterable} of another type or with
     *  each element modified in some fashion.
     *
     * @param iterable input
     * @param func mapping function applied to each element
     * @return a new iterable with each element mapped to a different value
     */
    public static <V, T> AsyncIterable<T> mapIterable(final AsyncIterable<V> iterable,
                                                      final Function<V, T> func) {
        return new AsyncIterable<T>() {
            @Override
            public AsyncIterator<T> iterator() {
                final AsyncIterator<V> it = iterable.iterator();
                return new AsyncIterator<T>() {

                    @Override
                    public void remove() {
                        it.remove();
                    }

                    @Override
                    public Future<Boolean> onHasNext() {
                        return it.onHasNext();
                    }

                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public T next() {
                        return func.apply(it.next());
                    }

                    @Override
                    public void cancel() {
                        it.cancel();
                    }

                    @Override
                    public void dispose() {
                        it.dispose();
                    }
                };
            }

            @Override
            public Future<List<T>> asList() {
                return iterable.asList().map(new Function<List<V>, List<T>>() {
                    @Override
                    public List<T> apply(List<V> o) {
                        ArrayList<T> out = new ArrayList<T>(o.size());
                        for(V in : o)
                            out.add(func.apply(in));
                        return out;
                    }
                });
            }
        };
    }
}
