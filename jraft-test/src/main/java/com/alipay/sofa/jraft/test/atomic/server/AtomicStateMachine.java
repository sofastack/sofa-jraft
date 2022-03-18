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
package com.alipay.sofa.jraft.test.atomic.server;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.test.atomic.KeyNotFoundException;
import com.alipay.sofa.jraft.test.atomic.command.BaseRequestCommand;
import com.alipay.sofa.jraft.test.atomic.command.BooleanCommand;
import com.alipay.sofa.jraft.test.atomic.command.CommandCodec;
import com.alipay.sofa.jraft.test.atomic.command.CompareAndSetCommand;
import com.alipay.sofa.jraft.test.atomic.command.GetCommand;
import com.alipay.sofa.jraft.test.atomic.command.IncrementAndGetCommand;
import com.alipay.sofa.jraft.test.atomic.command.SetCommand;
import com.alipay.sofa.jraft.test.atomic.command.ValueCommand;
import com.alipay.sofa.jraft.util.Utils;

/**
 * Atomic state machine
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-25 1:47:50 PM
 */
public class AtomicStateMachine extends StateMachineAdapter {

    private static final Logger                         LOG        = LoggerFactory.getLogger(AtomicStateMachine.class);

    // <key, counter>
    private final ConcurrentHashMap<String, AtomicLong> counters   = new ConcurrentHashMap<>();

    /**
     * leader term
     */
    private final AtomicLong                            leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public void onApply(final Iterator iter) {
        while (iter.hasNext()) {
            final Closure done = iter.done();
            CommandType cmdType;
            final ByteBuffer data = iter.getData();
            Object cmd = null;
            LeaderTaskClosure closure = null;
            if (done != null) {
                closure = (LeaderTaskClosure) done;
                cmdType = closure.getCmdType();
                cmd = closure.getCmd();
            } else {
                final byte b = data.get();
                final byte[] cmdBytes = new byte[data.remaining()];
                data.get(cmdBytes);
                cmdType = CommandType.parseByte(b);
                // follower ignore read operation
                if (cmdType.isReadOp()) {
                    iter.next();
                    continue;
                }
                switch (cmdType) {
                    case GET:
                        cmd = CommandCodec.decodeCommand(cmdBytes, GetCommand.class);
                        break;
                    case SET:
                        cmd = CommandCodec.decodeCommand(cmdBytes, SetCommand.class);
                        break;
                    case CAS:
                        cmd = CommandCodec.decodeCommand(cmdBytes, CompareAndSetCommand.class);
                        break;
                    case INC:
                        cmd = CommandCodec.decodeCommand(cmdBytes, IncrementAndGetCommand.class);
                        break;
                }
            }
            final String key = ((BaseRequestCommand) cmd).getKey();
            final AtomicLong counter = getCounter(key, true);
            Object response = null;
            switch (cmdType) {
                case GET:
                    response = new ValueCommand(counter.get());
                    break;
                case SET:
                    final SetCommand setCmd = (SetCommand) cmd;
                    counter.set(setCmd.getValue());
                    response = new BooleanCommand(true);
                    break;
                case CAS:
                    final CompareAndSetCommand casCmd = (CompareAndSetCommand) cmd;
                    response = new BooleanCommand(counter.compareAndSet(casCmd.getExpect(), casCmd.getNewValue()));
                    break;
                case INC:
                    final IncrementAndGetCommand incCmd = (IncrementAndGetCommand) cmd;
                    final long ret = counter.addAndGet(incCmd.getDetal());
                    response = new ValueCommand(ret);
                    break;
            }
            if (closure != null) {
                closure.setResponse(response);
                closure.run(Status.OK());
            }
            iter.next();
        }
    }

    private AtomicLong getCounter(final String key, final boolean createWhenNotFound) {
        AtomicLong ret = this.counters.get(key);
        if (ret == null && createWhenNotFound) {
            ret = new AtomicLong(0);
            final AtomicLong old = this.counters.putIfAbsent(key, ret);
            if (old != null) {
                ret = old;
            }
        }
        return ret;
    }

    public long getValue(final String key) throws KeyNotFoundException {
        final AtomicLong counter = getCounter(key, false);
        if (counter == null) {
            throw new KeyNotFoundException("Key `" + key + "` not found");
        }
        return counter.get();
    }

    @Override
  public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
    final Map<String, Long> values = new HashMap<>();
    for (final Map.Entry<String, AtomicLong> entry : this.counters.entrySet()) {
      values.put(entry.getKey(), entry.getValue().get());
    }
    Utils.runInThread(() -> {
      final AtomicSnapshotFile snapshot = new AtomicSnapshotFile(writer.getPath() + File.separator + "data");
      if (snapshot.save(values)) {
        if (writer.addFile("data")) {
          done.run(Status.OK());
        } else {
          done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
        }
      } else {
        done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
      }
    });
  }

    @Override
    public void onError(final RaftException e) {
        LOG.error("Raft error: {}", e, e);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot");
            return false;
        }
        if (reader.getFileMeta("data") == null) {
            LOG.error("Fail to find data file in {}", reader.getPath());
            return false;
        }
        final AtomicSnapshotFile snapshot = new AtomicSnapshotFile(reader.getPath() + File.separator + "data");
        try {
            final Map<String, Long> values = snapshot.load();
            this.counters.clear();
            if (values != null) {
                for (final Map.Entry<String, Long> entry : values.entrySet()) {
                    this.counters.put(entry.getKey(), new AtomicLong(entry.getValue()));
                }
            }
            return true;
        } catch (final IOException e) {
            LOG.error("Fail to load snapshot from {}", snapshot.getPath());
            return false;
        }

    }

    @Override
    public void onLeaderStart(final long term) {
        this.leaderTerm.set(term);
        super.onLeaderStart(term);

    }

    @Override
    public void onLeaderStop(final Status status) {
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

}
