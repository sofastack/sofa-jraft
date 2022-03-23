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
package com.alipay.sofa.jraft.storage.file.index;

/**
 * @author hzh (642256541@qq.com)
 */
public enum IndexType {

    IndexSegment((byte) 1), // Segment log' s index
    IndexConf((byte) 2); // Conf log's index

    private final byte indexType;

    IndexType(final byte indexType) {
        this.indexType = indexType;
    }

    public byte getType() {
        return indexType;
    }
}
