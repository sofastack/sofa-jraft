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
package com.alipay.sofa.jraft.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceConfigurationError;

/**
 * A simple service-provider loading facility (SPI).
 *
 * @author jiachun.fjc
 */
public final class JRaftServiceLoader<S> implements Iterable<S> {

    private static final String      PREFIX    = "META-INF/services/";

    // the class or interface representing the service being loaded
    private final Class<S>           service;

    // the class loader used to locate, load, and instantiate providers
    private final ClassLoader        loader;

    // cached providers, in instantiation order
    private LinkedHashMap<String, S> providers = new LinkedHashMap<>();

    // the current lazy-lookup iterator
    private LazyIterator             lookupIterator;

    public static <S> JRaftServiceLoader<S> load(final Class<S> service) {
        return JRaftServiceLoader.load(service, Thread.currentThread().getContextClassLoader());
    }

    public static <S> JRaftServiceLoader<S> load(final Class<S> service, final ClassLoader loader) {
        return new JRaftServiceLoader<>(service, loader);
    }

    public List<S> sort() {
        final Iterator<S> it = iterator();
        final List<S> sortList = new ArrayList<>();
        while (it.hasNext()) {
            sortList.add(it.next());
        }

        if (sortList.size() <= 1) {
            return sortList;
        }

        sortList.sort((o1, o2) -> {
            final SPI o1_spi = o1.getClass().getAnnotation(SPI.class);
            final SPI o2_spi = o2.getClass().getAnnotation(SPI.class);

            final int o1_priority = o1_spi == null ? 0 : o1_spi.priority();
            final int o2_priority = o2_spi == null ? 0 : o2_spi.priority();

            return -(o1_priority - o2_priority);
        });

        return sortList;
    }

    public S first() {
        final List<S> sortList = sort();
        if (sortList.isEmpty()) {
            throw fail(this.service, "could not find any implementation for class");
        }
        return sortList.get(0);
    }

    public S find(final String implName) {
        for (final S s : this.providers.values()) {
            final SPI spi = s.getClass().getAnnotation(SPI.class);
            if (spi != null && spi.name().equalsIgnoreCase(implName)) {
                return s;
            }
        }
        while (this.lookupIterator.hasNext()) {
            final Class<S> cls = this.lookupIterator.next();
            final SPI spi = cls.getAnnotation(SPI.class);
            if (spi != null && spi.name().equalsIgnoreCase(implName)) {
                try {
                    final S provider = this.service.cast(cls.newInstance());
                    this.providers.put(cls.getName(), provider);
                    return provider;
                } catch (Throwable x) {
                    throw fail(this.service, "provider " + cls.getName() + " could not be instantiated", x);
                }
            }
        }
        throw fail(this.service, "provider " + implName + " not found");
    }

    public void reload() {
        this.providers.clear();
        this.lookupIterator = new LazyIterator(this.service, this.loader);
    }

    private JRaftServiceLoader(final Class<S> service, final ClassLoader loader) {
        this.service = Requires.requireNonNull(service, "service interface cannot be null");
        this.loader = (loader == null) ? ClassLoader.getSystemClassLoader() : loader;
        reload();
    }

    private static ServiceConfigurationError fail(final Class<?> service, final String msg, final Throwable cause) {
        return new ServiceConfigurationError(service.getName() + ": " + msg, cause);
    }

    private static ServiceConfigurationError fail(final Class<?> service, final String msg) {
        return new ServiceConfigurationError(service.getName() + ": " + msg);
    }

    private static ServiceConfigurationError fail(final Class<?> service, final URL url, final int line,
                                                  final String msg) {
        return fail(service, url + ":" + line + ": " + msg);
    }

    // parse a single line from the given configuration file, adding the name
    // on the line to the names list.
    private int parseLine(final Class<?> service, final URL u, final BufferedReader r, final int lc,
                          final List<String> names) throws IOException, ServiceConfigurationError {

        String ln = r.readLine();
        if (ln == null) {
            return -1;
        }
        final int ci = ln.indexOf('#');
        if (ci >= 0) {
            ln = ln.substring(0, ci);
        }
        ln = ln.trim();
        final int n = ln.length();
        if (n != 0) {
            if ((ln.indexOf(' ') >= 0) || (ln.indexOf('\t') >= 0)) {
                throw fail(service, u, lc, "illegal configuration-file syntax");
            }
            int cp = ln.codePointAt(0);
            if (!Character.isJavaIdentifierStart(cp)) {
                throw fail(service, u, lc, "illegal provider-class name: " + ln);
            }
            for (int i = Character.charCount(cp); i < n; i += Character.charCount(cp)) {
                cp = ln.codePointAt(i);
                if (!Character.isJavaIdentifierPart(cp) && (cp != '.')) {
                    throw fail(service, u, lc, "Illegal provider-class name: " + ln);
                }
            }
            if (!this.providers.containsKey(ln) && !names.contains(ln)) {
                names.add(ln);
            }
        }
        return lc + 1;
    }

    private Iterator<String> parse(final Class<?> service, final URL url) {
        final ArrayList<String> names = new ArrayList<>();
        try (final InputStream in = url.openStream();
                final BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            int lc = 1;
            // noinspection StatementWithEmptyBody
            while ((lc = parseLine(service, url, r, lc, names)) >= 0)
                ;
        } catch (final IOException x) {
            throw fail(service, "error reading configuration file", x);
        }
        return names.iterator();
    }

    @Override
    public Iterator<S> iterator() {
        return new Iterator<S>() {

            final Iterator<Map.Entry<String, S>> knownProviders = JRaftServiceLoader.this.providers.entrySet()
                                                                    .iterator();

            @Override
            public boolean hasNext() {
                return this.knownProviders.hasNext() || JRaftServiceLoader.this.lookupIterator.hasNext();
            }

            @Override
            public S next() {
                if (this.knownProviders.hasNext()) {
                    return this.knownProviders.next().getValue();
                }
                final Class<S> cls = JRaftServiceLoader.this.lookupIterator.next();
                try {
                    final S provider = JRaftServiceLoader.this.service.cast(cls.newInstance());
                    JRaftServiceLoader.this.providers.put(cls.getName(), provider);
                    return provider;
                } catch (final Throwable x) {
                    throw fail(JRaftServiceLoader.this.service, "provider " + cls.getName()
                                                                + " could not be instantiated", x);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private class LazyIterator implements Iterator<Class<S>> {
        Class<S>         service;
        ClassLoader      loader;
        Enumeration<URL> configs  = null;
        Iterator<String> pending  = null;
        String           nextName = null;

        private LazyIterator(Class<S> service, ClassLoader loader) {
            this.service = service;
            this.loader = loader;
        }

        @Override
        public boolean hasNext() {
            if (this.nextName != null) {
                return true;
            }
            if (this.configs == null) {
                try {
                    final String fullName = PREFIX + this.service.getName();
                    if (this.loader == null) {
                        this.configs = ClassLoader.getSystemResources(fullName);
                    } else {
                        this.configs = this.loader.getResources(fullName);
                    }
                } catch (final IOException x) {
                    throw fail(this.service, "error locating configuration files", x);
                }
            }
            while ((this.pending == null) || !this.pending.hasNext()) {
                if (!this.configs.hasMoreElements()) {
                    return false;
                }
                this.pending = parse(this.service, this.configs.nextElement());
            }
            this.nextName = this.pending.next();
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Class<S> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final String name = this.nextName;
            this.nextName = null;
            final Class<?> cls;
            try {
                cls = Class.forName(name, false, this.loader);
            } catch (final ClassNotFoundException x) {
                throw fail(this.service, "provider " + name + " not found");
            }
            if (!this.service.isAssignableFrom(cls)) {
                throw fail(this.service, "provider " + name + " not a subtype");
            }
            return (Class<S>) cls;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Returns a string describing this service.
     */
    @Override
    public String toString() {
        return "com.alipay.sofa.jraft.util.JRaftServiceLoader[" + this.service.getName() + "]";
    }
}
