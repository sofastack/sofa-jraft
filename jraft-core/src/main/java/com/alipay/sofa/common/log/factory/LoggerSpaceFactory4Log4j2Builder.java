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
package com.alipay.sofa.common.log.factory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;

import com.alipay.sofa.common.log.SpaceInfo;
import com.alipay.sofa.common.log.adapter.level.AdapterLevel;

/**
 * Compatibility override for Java 8 projects upgrading to slf4j 2.x.
 * sofa-common-tools 1.0.12 hardcodes the old Log4jLogger constructor removed by log4j-slf4j2-impl.
 */
public class LoggerSpaceFactory4Log4j2Builder extends AbstractLoggerSpaceFactoryBuilder {

    public LoggerSpaceFactory4Log4j2Builder(final SpaceInfo spaceInfo) {
        super(spaceInfo);
    }

    @Override
    protected String getLoggingToolName() {
        return "log4j2";
    }

    @Override
    public AbstractLoggerSpaceFactory doBuild(final String spaceName, final ClassLoader classLoader, final URL url) {
        try {
            final Object context = newLoggerContext(spaceName, url.toURI());
            final Object configuration = getConfiguration(spaceName, classLoader, url.toURI(), context);
            if (configuration == null) {
                throw new RuntimeException("No log4j2 configuration are found.");
            }

            mergeProperties(configuration, getProperties());
            mergeProperties(configuration, System.getProperties());
            invoke(context, "start", new Class<?>[] { configurationClass() }, configuration);

            return new AbstractLoggerSpaceFactory(getLoggingToolName()) {
                private final ConcurrentMap<String, Logger> loggerMap     = new ConcurrentHashMap<>();
                private final Object                        markerFactory = newLog4jMarkerFactory();

                @Override
                public Logger setLevel(final String loggerName, final AdapterLevel level) {
                    final String actualLoggerName = "ROOT".equals(loggerName) ? "" : loggerName;
                    final Object logger = invoke(context, "getLogger", new Class<?>[] { String.class },
                        actualLoggerName);
                    invoke(logger, "setLevel", new Class<?>[] { levelClass() }, toLog4j2Level(level));
                    return getLogger(loggerName);
                }

                @Override
                public Logger getLogger(final String loggerName) {
                    final String actualLoggerName = "ROOT".equals(loggerName) ? "" : loggerName;
                    final Logger existing = this.loggerMap.get(actualLoggerName);
                    if (existing != null) {
                        return existing;
                    }

                    final Object extendedLogger = invoke(context, "getLogger", new Class<?>[] { String.class },
                        actualLoggerName);
                    final Logger created = newLog4jLogger(this.markerFactory, extendedLogger, actualLoggerName);
                    final Logger raced = this.loggerMap.putIfAbsent(actualLoggerName, created);
                    return raced == null ? created : raced;
                }

                private Object toLog4j2Level(final AdapterLevel level) {
                    if (level == null) {
                        return enumValue("org.apache.logging.log4j.Level", "INFO");
                    }
                    switch (level) {
                        case ERROR:
                            return enumValue("org.apache.logging.log4j.Level", "ERROR");
                        case WARN:
                            return enumValue("org.apache.logging.log4j.Level", "WARN");
                        case INFO:
                            return enumValue("org.apache.logging.log4j.Level", "INFO");
                        case DEBUG:
                            return enumValue("org.apache.logging.log4j.Level", "DEBUG");
                        case TRACE:
                            return enumValue("org.apache.logging.log4j.Level", "TRACE");
                        default:
                            return enumValue("org.apache.logging.log4j.Level", "INFO");
                    }
                }
            };
        } catch (final Throwable t) {
            throw new IllegalStateException("Log4j2 loggerSpaceFactory build error!", t);
        }
    }

    private static Object getConfiguration(final String spaceName, final ClassLoader classLoader, final URI uri,
                                           final Object context) throws Exception {
        final Object configurationFactory = invokeStatic(configurationFactoryClass(), "getInstance", new Class<?>[0]);
        try {
            final Method method = configurationFactory.getClass().getMethod("getConfiguration", String.class,
                URI.class, ClassLoader.class);
            return method.invoke(configurationFactory, spaceName, uri, classLoader);
        } catch (final NoSuchMethodException ignored) {
            final Method method = configurationFactory.getClass().getMethod("getConfiguration", loggerContextClass(),
                String.class, URI.class, ClassLoader.class);
            return method.invoke(configurationFactory, context, spaceName, uri, classLoader);
        }
    }

    private static void mergeProperties(final Object configuration, final Properties properties) {
        @SuppressWarnings("unchecked")
        final Map<String, String> configurationProperties = (Map<String, String>) invoke(configuration,
            "getProperties", new Class<?>[0]);
        for (final Map.Entry<Object, Object> entry : properties.entrySet()) {
            configurationProperties.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
    }

    private static Object newLoggerContext(final String spaceName, final URI uri) throws Exception {
        final Constructor<?> constructor = loggerContextClass().getConstructor(String.class, Object.class, URI.class);
        return constructor.newInstance(spaceName, null, uri);
    }

    private static Object newLog4jMarkerFactory() {
        return newInstance("org.apache.logging.slf4j.Log4jMarkerFactory");
    }

    private static Logger newLog4jLogger(final Object markerFactory, final Object extendedLogger,
                                         final String loggerName) {
        return (Logger) newInstance("org.apache.logging.slf4j.Log4jLogger", new Class<?>[] {
                loadClass("org.apache.logging.slf4j.Log4jMarkerFactory"),
                loadClass("org.apache.logging.log4j.spi.ExtendedLogger"), String.class }, markerFactory,
            extendedLogger, loggerName);
    }

    private static Object enumValue(final String className, final String constant) {
        @SuppressWarnings({ "rawtypes", "unchecked" })
        final Object value = Enum.valueOf((Class) loadClass(className), constant);
        return value;
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>[] parameterTypes,
                                 final Object... args) {
        try {
            final Method method = target.getClass().getMethod(methodName, parameterTypes);
            return method.invoke(target, args);
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Object invokeStatic(final Class<?> type, final String methodName, final Class<?>[] parameterTypes,
                                       final Object... args) {
        try {
            final Method method = type.getMethod(methodName, parameterTypes);
            return method.invoke(null, args);
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Object newInstance(final String className) {
        return newInstance(className, new Class<?>[0]);
    }

    private static Object newInstance(final String className, final Class<?>[] parameterTypes, final Object... args) {
        try {
            final Constructor<?> constructor = loadClass(className).getConstructor(parameterTypes);
            return constructor.newInstance(args);
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Class<?> configurationFactoryClass() {
        return loadClass("org.apache.logging.log4j.core.config.ConfigurationFactory");
    }

    private static Class<?> configurationClass() {
        return loadClass("org.apache.logging.log4j.core.config.Configuration");
    }

    private static Class<?> loggerContextClass() {
        return loadClass("org.apache.logging.log4j.core.LoggerContext");
    }

    private static Class<?> levelClass() {
        return loadClass("org.apache.logging.log4j.Level");
    }

    private static Class<?> loadClass(final String className) {
        try {
            return Class.forName(className);
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
}
