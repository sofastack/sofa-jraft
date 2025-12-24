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
package com.alipay.sofa.jraft.closure;

import com.alipay.sofa.jraft.Closure;

/**
 * Marker interface for framework-internal closures.
 * <p>
 * Internal closures are executed on a separate executor pool to isolate
 * them from user task closures, improving performance and preventing
 * user tasks from blocking internal operations.
 *
 * @author dennis
 * @since 1.4.1
 */
public interface InternalClosure extends Closure {
    // Marker interface - no additional methods
}
