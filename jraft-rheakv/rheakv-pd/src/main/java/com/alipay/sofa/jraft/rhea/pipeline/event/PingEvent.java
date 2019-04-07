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
package com.alipay.sofa.jraft.rhea.pipeline.event;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingDeque;

import com.alipay.sofa.jraft.rhea.MetadataStore;
import com.alipay.sofa.jraft.rhea.metadata.Instruction;
import com.alipay.sofa.jraft.rhea.util.pipeline.event.InboundMessageEvent;

/**
 * @author jiachun.fjc
 */
public abstract class PingEvent<T> extends InboundMessageEvent<T> {

    private final Collection<Instruction> instructions = new LinkedBlockingDeque<>();
    private final MetadataStore           metadataStore;

    public PingEvent(T message, MetadataStore metadataStore) {
        super(message);
        this.metadataStore = metadataStore;
    }

    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    public Collection<Instruction> getInstructions() {
        return instructions;
    }

    public void addInstruction(Instruction instruction) {
        this.instructions.add(instruction);
    }

    public boolean isReady() {
        return !this.instructions.isEmpty();
    }
}
