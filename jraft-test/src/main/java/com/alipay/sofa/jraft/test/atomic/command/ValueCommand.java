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
package com.alipay.sofa.jraft.test.atomic.command;

/**
 * Value response command
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-25 1:29:03 PM
 */
public class ValueCommand extends BooleanCommand {
    private static final long serialVersionUID = -4313480716428249772L;
    private long              vlaue;

    public ValueCommand() {
        super();
    }

    public ValueCommand(boolean result, String errorMsg) {
        super(result, errorMsg);
    }

    public ValueCommand(boolean result) {
        super(result);
    }

    public ValueCommand(long vlaue) {
        super();
        this.vlaue = vlaue;
        this.setSuccess(true);
    }

    public long getVlaue() {
        return this.vlaue;
    }

    public void setVlaue(long vlaue) {
        this.vlaue = vlaue;
    }

}
