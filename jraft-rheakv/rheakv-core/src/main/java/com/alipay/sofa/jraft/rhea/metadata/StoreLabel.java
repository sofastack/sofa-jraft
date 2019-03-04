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
package com.alipay.sofa.jraft.rhea.metadata;

import java.io.Serializable;

import com.alipay.sofa.jraft.util.Copiable;

/**
 * Case insensitive key/value for replica constraints.
 *
 * @author jiachun.fjc
 */
public class StoreLabel implements Copiable<StoreLabel>, Serializable {

    private static final long serialVersionUID = 5672444723199723795L;

    private String            key;
    private String            value;

    public StoreLabel() {
    }

    public StoreLabel(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public StoreLabel copy() {
        return new StoreLabel(this.key, this.value);
    }

    @Override
    public String toString() {
        return "StoreLabel{" + "key='" + key + '\'' + ", value='" + value + '\'' + '}';
    }
}
