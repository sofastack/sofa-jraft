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
package com.alipay.sofa.jraft.test.atomic.server.processor;

import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.SetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.IncrementAndGetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetSlotsCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.CompareAndSetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.GetCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseResponseCommand;
import com.alipay.sofa.jraft.test.atomic.command.RpcCommand.BaseRequestCommand;

public interface KVService {

    void handleGetCommand(final BaseRequestCommand baseReqCmd, final GetCommand getCmd,
                          final RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure);

    void handleCompareAndSetCommand(final BaseRequestCommand baseReqCmd, final CompareAndSetCommand compAndSetCmd,
                                    final RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure);

    void handleGetSlotsCommand(final BaseRequestCommand baseReqCmd, final GetSlotsCommand getSlotsCmd,
                               final RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure);

    void handleIncrementAndGetCommand(final BaseRequestCommand baseReqCmd, final IncrementAndGetCommand increAndGetCmd,
                                      final RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure);

    void handleSetCommand(final BaseRequestCommand baseReqCmd, final SetCommand setCmd,
                          final RequestProcessClosure<BaseRequestCommand, BaseResponseCommand> closure);
}
