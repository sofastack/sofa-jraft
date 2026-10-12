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
package com.alipay.sofa.jraft.core;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.Quorum;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.SynchronizedClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.rpc.RpcRequests.AppendEntriesRequest;
import com.alipay.sofa.jraft.rpc.RpcRequests.RequestVoteRequest;
import com.alipay.sofa.jraft.rpc.impl.AbstractClientService;
import com.alipay.sofa.jraft.test.TestUtils;
import com.google.protobuf.Message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NwrQuorumTest {

    private static final int NODE_COUNT = 4;
    private static final int FIRST_LEADER = 0;
    private static final int SECOND_LEADER = 3;
    private static final int FIRST_VOTER = 1;
    private static final int SECOND_VOTER = 2;
    private static final int ELECTION_TIMEOUT_MS = 60_000;
    private static final long WAIT_SECONDS = 10;
    private static final long DATA_INDEX = 2;

    private final List<NodeImpl> nodes = new ArrayList<>();
    private final Map<String, NodeImpl> nodesById = new HashMap<>();
    private final Map<Integer, SynchronizedClosure> completions = new HashMap<>();
    private final LinkedBlockingQueue<PendingRpc> requests = new LinkedBlockingQueue<>();
    private final List<PendingRpc> delayed = new ArrayList<>();
    private Path dataPath;
    private boolean passed;

    @Before
    public void setup() throws Exception {
        this.dataPath = Files.createTempDirectory("jraft-nwr-");
    }

    @After
    public void teardown() throws Exception {
        for (final NodeImpl node : this.nodes) {
            node.shutdown();
        }
        final NodeManager manager = NodeManager.getInstance();
        for (final NodeImpl node : this.nodes) {
            node.join();
            final PeerId peer = node.getServerId();
            manager.removeAddress(peer.getEndpoint());
        }
        if (this.passed) {
            FileUtils.deleteDirectory(this.dataPath.toFile());
        }
    }

    @Test(timeout = 30_000)
    public void testCommittedEntriesMustMatchWithFlexibleQuorum() throws Exception {
        startNodes();
        this.nodes.get(FIRST_LEADER).tryElectSelf();
        this.nodes.get(SECOND_LEADER).tryElectSelf();
        deliverVote(FIRST_LEADER, FIRST_VOTER);
        deliverVote(SECOND_LEADER, SECOND_VOTER);
        final boolean firstLeader = this.nodes.get(FIRST_LEADER).isLeader();
        final boolean secondLeader = this.nodes.get(SECOND_LEADER).isLeader();
        if (firstLeader && secondLeader) {
            assertMatchingCommittedValues();
        }
        assertCandidate(FIRST_LEADER, 1, 0);
        assertCandidate(SECOND_LEADER, 1, 0);
        this.passed = true;
    }

    @Test(timeout = 30_000)
    public void testElectionNeedsThreeVotesButCommitNeedsTwoCopies() throws Exception {
        startNodes();
        this.nodes.get(FIRST_LEADER).tryElectSelf();
        deliverVote(FIRST_LEADER, FIRST_VOTER);
        assertCandidate(FIRST_LEADER, 1, 0);
        deliverVote(FIRST_LEADER, SECOND_VOTER);
        assertLeader(FIRST_LEADER);
        replicateUntilApplied(FIRST_LEADER, 1);
        applyValue(FIRST_LEADER, "x");
        replicateUntilApplied(FIRST_LEADER, DATA_INDEX);
        assertCommitted(FIRST_LEADER, "x");
        this.passed = true;
    }

    @Test(timeout = 30_000)
    public void testNewElectionDoesNotReuseEarlierVotes() throws Exception {
        startNodes();
        final NodeImpl node = this.nodes.get(FIRST_LEADER);
        node.tryElectSelf();
        deliverVote(FIRST_LEADER, FIRST_VOTER);
        final RequestVoteRequest.Builder builder = RequestVoteRequest.newBuilder();
        builder.setGroupId(node.getGroupId());
        builder.setServerId(serverId(SECOND_LEADER));
        builder.setPeerId(serverId(FIRST_LEADER));
        builder.setTerm(2);
        builder.setLastLogIndex(0);
        builder.setLastLogTerm(0);
        builder.setPreVote(false);
        final RequestVoteRequest request = builder.build();
        node.handleRequestVoteRequest(request);
        node.tryElectSelf();
        deliverVote(FIRST_LEADER, SECOND_VOTER);
        assertCandidate(FIRST_LEADER, 3, 0);
        this.passed = true;
    }

    private void assertMatchingCommittedValues() throws Exception {
        // Replicate 1 -> 3 and 4 -> 2; each follower voted for the other leader.
        replicateUntilApplied(FIRST_LEADER, 1);
        replicateUntilApplied(SECOND_LEADER, 1);
        applyValue(FIRST_LEADER, "x");
        applyValue(SECOND_LEADER, "y");
        replicateUntilApplied(FIRST_LEADER, DATA_INDEX);
        replicateUntilApplied(SECOND_LEADER, DATA_INDEX);
        assertCommitted(FIRST_LEADER, "x");
        assertCommitted(SECOND_LEADER, "y");
        final List<String> firstValues = committedValues(FIRST_LEADER);
        final List<String> secondValues = committedValues(SECOND_LEADER);
        assertEquals("Committed values at index 2 must match in the same term", firstValues, secondValues);
    }

    private void startNodes() throws Exception {
        final List<PeerId> peers = TestUtils.generatePeers(NODE_COUNT);
        final NodeManager manager = NodeManager.getInstance();
        for (final PeerId peer : peers) {
            final NodeOptions options = newOptions(peer, peers);
            final Quorum quorum = options.getInitialConf().getQuorum();
            assertEquals(3, quorum.getR());
            assertEquals(2, quorum.getW());
            final NodeImpl node = new NodeImpl("nwr-repro", peer);
            this.nodes.add(node);
            this.nodesById.put(peer.toString(), node);
            manager.addAddress(peer.getEndpoint());
            final boolean initialized = node.init(options);
            assertTrue(initialized);
            installDelayedTransport(node);
        }
    }

    private NodeOptions newOptions(final PeerId peer, final List<PeerId> peers) throws IOException {
        final String address = peer.getEndpoint().toString();
        final String directory = address.replace(':', '_');
        final Path nodePath = this.dataPath.resolve(directory);
        Files.createDirectory(nodePath);
        final NodeOptions options = new NodeOptions();
        options.setLogUri(nodePath.resolve("logs").toString());
        options.setRaftMetaUri(nodePath.resolve("meta").toString());
        options.setElectionTimeoutMs(ELECTION_TIMEOUT_MS);
        options.setSnapshotIntervalSecs(0);
        final Configuration conf = new Configuration(peers);
        options.setInitialConf(conf);
        options.enableFlexibleRaft(true);
        options.setFactor(6, 4);
        final MockStateMachine fsm = new MockStateMachine(peer.getEndpoint());
        options.setFsm(fsm);
        return options;
    }

    private void installDelayedTransport(final NodeImpl node) throws Exception {
        final AbstractClientService service = (AbstractClientService) node.getRpcService();
        final RpcClient original = service.getRpcClient();
        original.shutdown();
        final Answer<Object> handler = this::captureRpc;
        final RpcClient transport = Mockito.mock(RpcClient.class, handler);
        final Field field = AbstractClientService.class.getDeclaredField("rpcClient");
        field.setAccessible(true);
        field.set(service, transport);
    }

    private Object captureRpc(final InvocationOnMock invocation) {
        final String method = invocation.getMethod().getName();
        if ("checkConnection".equals(method)) {
            return true;
        }
        if ("shutdown".equals(method)) {
            return null;
        }
        if (!"invokeAsync".equals(method)) {
            throw new UnsupportedOperationException(method);
        }
        final Object[] args = invocation.getArguments();
        final Message request = (Message) args[1];
        final InvokeCallback callback = (InvokeCallback) args[3];
        this.requests.add(new PendingRpc(request, callback));
        return null;
    }

    private void deliverVote(final int candidate, final int voter) throws Exception {
        final String source = serverId(candidate);
        final String target = serverId(voter);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            for (final PendingRpc rpc : this.requests) {
                if (isVoteFor(rpc.request, source, target)) {
                    this.requests.remove(rpc);
                    final RequestVoteRequest request = (RequestVoteRequest) rpc.request;
                    final NodeImpl node = this.nodes.get(voter);
                    final Message response = node.handleRequestVoteRequest(request);
                    rpc.callback.complete(response, null);
                    return;
                }
            }
            Thread.sleep(1);
        }
        throw new AssertionError("Missing RequestVote " + source + " -> " + target);
    }

    private boolean isVoteFor(final Message message, final String source, final String target) {
        if (!(message instanceof RequestVoteRequest)) {
            return false;
        }
        final RequestVoteRequest request = (RequestVoteRequest) message;
        final NodeImpl candidate = this.nodesById.get(source);
        final long term = candidate.getCurrentTerm();
        final boolean fromCandidate = source.equals(request.getServerId());
        final boolean toVoter = target.equals(request.getPeerId());
        final boolean correctRoute = fromCandidate && toVoter;
        final boolean currentTerm = request.getTerm() == term;
        return correctRoute && currentTerm;
    }

    private void replicateUntilApplied(final int leader, final long index) throws Exception {
        final NodeImpl node = this.nodes.get(leader);
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            final long appliedIndex = node.getLastAppliedLogIndex();
            if (appliedIndex == index) {
                return;
            }
            final PendingRpc rpc = this.requests.poll(10, TimeUnit.MILLISECONDS);
            if (rpc == null) {
                continue;
            }
            if (isCrossReplication(rpc.request)) {
                deliverAppend(rpc);
            } else {
                this.delayed.add(rpc);
            }
        }
        throw new AssertionError("Leader " + leader + " did not apply index " + index + "; logs: " + this.dataPath);
    }

    private boolean isCrossReplication(final Message message) {
        if (!(message instanceof AppendEntriesRequest)) {
            return false;
        }
        final AppendEntriesRequest request = (AppendEntriesRequest) message;
        final String source = request.getServerId();
        final String target = request.getPeerId();
        final boolean firstSource = source.equals(serverId(FIRST_LEADER));
        final boolean firstTarget = target.equals(serverId(SECOND_VOTER));
        final boolean secondSource = source.equals(serverId(SECOND_LEADER));
        final boolean secondTarget = target.equals(serverId(FIRST_VOTER));
        return firstSource && firstTarget || secondSource && secondTarget;
    }

    private void deliverAppend(final PendingRpc rpc) {
        final AppendEntriesRequest request = (AppendEntriesRequest) rpc.request;
        final NodeImpl node = this.nodesById.get(request.getPeerId());
        final Answer<Object> handler = invocation -> {
            final Object response = invocation.getArguments()[0];
            rpc.callback.complete(response, null);
            return null;
        };
        final RpcContext context = Mockito.mock(RpcContext.class, handler);
        final RpcRequestClosure done = new RpcRequestClosure(context);
        final Message response = node.handleAppendEntriesRequest(request, done);
        if (response != null) {
            rpc.callback.complete(response, null);
        }
    }

    private void applyValue(final int leader, final String value) {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer data = ByteBuffer.wrap(bytes);
        final SynchronizedClosure done = new SynchronizedClosure();
        this.completions.put(leader, done);
        final Task task = new Task(data, done);
        final NodeImpl node = this.nodes.get(leader);
        node.apply(task);
    }

    private void assertLeader(final int leader) {
        final NodeImpl node = this.nodes.get(leader);
        final State state = node.getNodeState();
        assertEquals(State.STATE_LEADER, state);
        final long term = node.getCurrentTerm();
        assertEquals(1, term);
    }

    private void assertCandidate(final int candidate, final long expectedTerm, final long expectedCommittedIndex) {
        final NodeImpl node = this.nodes.get(candidate);
        final State state = node.getNodeState();
        assertEquals(State.STATE_CANDIDATE, state);
        final long term = node.getCurrentTerm();
        assertEquals(expectedTerm, term);
        final long committedIndex = node.getLastCommittedIndex();
        assertEquals(expectedCommittedIndex, committedIndex);
    }

    private void assertCommitted(final int leader, final String value) throws InterruptedException {
        assertLeader(leader);
        final SynchronizedClosure done = this.completions.get(leader);
        final Status expectedStatus = Status.OK();
        final Status actualStatus = done.await();
        assertEquals(expectedStatus, actualStatus);
        final NodeImpl node = this.nodes.get(leader);
        final long committedIndex = node.getLastCommittedIndex();
        assertEquals(DATA_INDEX, committedIndex);
        final List<String> expected = Collections.singletonList(value);
        final List<String> actual = committedValues(leader);
        assertEquals(expected, actual);
    }

    private List<String> committedValues(final int index) {
        final NodeImpl node = this.nodes.get(index);
        final MockStateMachine fsm = (MockStateMachine) node.getOptions().getFsm();
        final List<ByteBuffer> logs = fsm.getLogs();
        final List<String> values = new ArrayList<>();
        for (final ByteBuffer log : logs) {
            final byte[] bytes = new byte[log.remaining()];
            final ByteBuffer data = log.duplicate();
            data.get(bytes);
            final String value = new String(bytes, StandardCharsets.UTF_8);
            values.add(value);
        }
        return values;
    }

    private String serverId(final int index) {
        final NodeImpl node = this.nodes.get(index);
        final PeerId peer = node.getServerId();
        return peer.toString();
    }

    private static class PendingRpc {
        final Message request;
        final InvokeCallback callback;

        PendingRpc(final Message request, final InvokeCallback callback) {
            this.request = request;
            this.callback = callback;
        }
    }
}
