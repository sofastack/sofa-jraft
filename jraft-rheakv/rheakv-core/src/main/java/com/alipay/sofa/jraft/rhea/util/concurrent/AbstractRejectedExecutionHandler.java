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
package com.alipay.sofa.jraft.rhea.util.concurrent;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rhea.util.JvmTools;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.Utils;

/**
 *
 * @author jiachun.fjc
 */
public abstract class AbstractRejectedExecutionHandler implements RejectedExecutionHandler {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractRejectedExecutionHandler.class);

    protected final String        threadPoolName;
    private final AtomicBoolean   dumpNeeded;
    private final String          dumpPrefixName;

    public AbstractRejectedExecutionHandler(String threadPoolName, boolean dumpNeeded, String dumpPrefixName) {
        this.threadPoolName = threadPoolName;
        this.dumpNeeded = new AtomicBoolean(dumpNeeded);
        this.dumpPrefixName = dumpPrefixName;
    }

    public void dumpJvmInfoIfNeeded() {
        if (this.dumpNeeded.getAndSet(false)) {
            final String now = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
            final String name = this.threadPoolName + "_" + now;
            try (final FileOutputStream fileOutput = new FileOutputStream(new File(this.dumpPrefixName + "_dump_"
                                                                                   + name + ".log"))) {

                final List<String> stacks = JvmTools.jStack();
                for (final String s : stacks) {
                    fileOutput.write(Utils.getBytes(s));
                }

                final List<String> memoryUsages = JvmTools.memoryUsage();
                for (final String m : memoryUsages) {
                    fileOutput.write(Utils.getBytes(m));
                }

                if (JvmTools.memoryUsed() > 0.9) {
                    JvmTools.jMap(this.dumpPrefixName + "_dump_" + name + ".bin", false);
                }
            } catch (final Throwable t) {
                LOG.error("Dump jvm info error: {}.", StackTraceUtil.stackTrace(t));
            }
        }
    }
}
