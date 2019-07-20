/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rhea.util;

import java.io.Serializable;
import java.util.AbstractList;
import java.util.AbstractSequentialList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.RandomAccess;
import java.util.function.Function;

import com.alipay.sofa.jraft.util.Ints;
import com.alipay.sofa.jraft.util.Requires;

/**
 * Static utility methods pertaining to {@link List} instances.
 *
 * @author jiachun.fjc
 */
public final class Lists {

    /**
     * Creates a mutable, empty {@code ArrayList} instance.
     */
    public static <E> ArrayList<E> newArrayList() {
        return new ArrayList<>();
    }

    /**
     * Creates a mutable {@code ArrayList} instance containing the given elements.
     */
    @SuppressWarnings("unchecked")
    public static <E> ArrayList<E> newArrayList(final E... elements) {
        Requires.requireNonNull(elements, "elements");
        // Avoid integer overflow when a large array is passed in
        final int capacity = computeArrayListCapacity(elements.length);
        final ArrayList<E> list = new ArrayList<>(capacity);
        Collections.addAll(list, elements);
        return list;
    }

    /**
     * Creates a mutable {@code ArrayList} instance containing the given elements.
     */
    @SuppressWarnings("unchecked")
    public static <E> ArrayList<E> newArrayList(final Iterable<? extends E> elements) {
        Requires.requireNonNull(elements, "elements");
        return elements instanceof Collection ? new ArrayList((Collection<E>) elements) : newArrayList(elements
            .iterator());
    }

    /**
     * Creates a mutable {@code ArrayList} instance containing the given elements.
     */
    public static <E> ArrayList<E> newArrayList(final Iterator<? extends E> elements) {
        final ArrayList<E> list = newArrayList();
        while (elements.hasNext()) {
            list.add(elements.next());
        }
        return list;
    }

    /**
     * Creates an {@code ArrayList} instance backed by an array of the exact size specified;
     * equivalent to {@link ArrayList#ArrayList(int)}.
     */
    public static <E> ArrayList<E> newArrayListWithCapacity(final int initialArraySize) {
        Requires.requireTrue(initialArraySize >= 0, "initialArraySize");
        return new ArrayList<>(initialArraySize);
    }

    /**
     * Returns a list that applies {@code function} to each element of {@code fromList}.
     * The returned list is a transformed view of {@code fromList};
     * changes to {@code fromList} will be reflected in the returned list and vice versa.
     */
    public static <F, T> List<T> transform(final List<F> fromList, final Function<? super F, ? extends T> function) {
        return (fromList instanceof RandomAccess) ? new TransformingRandomAccessList<>(fromList, function)
            : new TransformingSequentialList<>(fromList, function);
    }

    private static class TransformingRandomAccessList<F, T> extends AbstractList<T> implements RandomAccess,
                                                                                   Serializable {
        private static final long              serialVersionUID = 0;

        final List<F>                          fromList;
        final Function<? super F, ? extends T> function;

        TransformingRandomAccessList(List<F> fromList, Function<? super F, ? extends T> function) {
            this.fromList = Requires.requireNonNull(fromList, "fromList");
            this.function = Requires.requireNonNull(function, "function");
        }

        @Override
        public void clear() {
            fromList.clear();
        }

        @Override
        public T get(final int index) {
            return function.apply(fromList.get(index));
        }

        @Override
        public boolean isEmpty() {
            return fromList.isEmpty();
        }

        @Override
        public T remove(final int index) {
            return function.apply(fromList.remove(index));
        }

        @Override
        public int size() {
            return fromList.size();
        }
    }

    private static class TransformingSequentialList<F, T> extends AbstractSequentialList<T> implements Serializable {
        private static final long              serialVersionUID = 0;

        final List<F>                          fromList;
        final Function<? super F, ? extends T> function;

        TransformingSequentialList(List<F> fromList, Function<? super F, ? extends T> function) {
            this.fromList = Requires.requireNonNull(fromList, "fromList");
            this.function = Requires.requireNonNull(function, "function");
        }

        @Override
        public void clear() {
            fromList.clear();
        }

        @Override
        public int size() {
            return fromList.size();
        }

        @Override
        public ListIterator<T> listIterator(final int index) {
            return new TransformedListIterator<F, T>(fromList.listIterator(index)) {

                @Override
                T transform(F from) {
                    return function.apply(from);
                }
            };
        }
    }

    abstract static class TransformedIterator<F, T> implements Iterator<T> {

        final Iterator<? extends F> backingIterator;

        TransformedIterator(Iterator<? extends F> backingIterator) {
            this.backingIterator = Requires.requireNonNull(backingIterator, "backingIterator");
        }

        abstract T transform(final F from);

        @Override
        public final boolean hasNext() {
            return backingIterator.hasNext();
        }

        @Override
        public final T next() {
            return transform(backingIterator.next());
        }

        @Override
        public final void remove() {
            backingIterator.remove();
        }
    }

    abstract static class TransformedListIterator<F, T> extends TransformedIterator<F, T> implements ListIterator<T> {

        TransformedListIterator(ListIterator<? extends F> backingIterator) {
            super(backingIterator);
        }

        @SuppressWarnings("unchecked")
        private ListIterator<? extends F> backingIterator() {
            return (ListIterator<? extends F>) backingIterator;
        }

        @Override
        public final boolean hasPrevious() {
            return backingIterator().hasPrevious();
        }

        @Override
        public final T previous() {
            return transform(backingIterator().previous());
        }

        @Override
        public final int nextIndex() {
            return backingIterator().nextIndex();
        }

        @Override
        public final int previousIndex() {
            return backingIterator().previousIndex();
        }

        @Override
        public void set(final T element) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void add(final T element) {
            throw new UnsupportedOperationException();
        }
    }

    static int computeArrayListCapacity(final int arraySize) {
        Requires.requireTrue(arraySize >= 0, "arraySize");
        return Ints.saturatedCast(5L + arraySize + (arraySize / 10));
    }

    private Lists() {
    }
}
