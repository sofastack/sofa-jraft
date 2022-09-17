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
package com.alipay.sofa.jraft;

import java.util.List;

import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.core.Replicator;
import com.alipay.sofa.jraft.core.State;
import com.alipay.sofa.jraft.entity.NodeId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.entity.UserLog;
import com.alipay.sofa.jraft.error.LogIndexOutOfBoundsException;
import com.alipay.sofa.jraft.error.LogNotFoundException;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.ReadOnlyOption;
import com.alipay.sofa.jraft.util.Describer;

/**
 * A raft replica node.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 4:06:55 PM
 */
public interface Node extends Lifecycle<NodeOptions>, Describer {

    /**
     * Get the leader peer id for redirect, null if absent.
     */
    PeerId getLeaderId();

    /**
     * Get current node id.
     */
    NodeId getNodeId();

    /**
     * Get the node metrics, only valid when node option {@link NodeOptions#isEnableMetrics()} is true.
     */
    NodeMetrics getNodeMetrics();

    /**
     * Get the raft group id.
     */
    String getGroupId();

    /**
     * Get the node options.
     */
    NodeOptions getOptions();

    /**
     * Get the raft options
     */
    RaftOptions getRaftOptions();

    /**
     * Returns true when the node is leader.
     */
    boolean isLeader();

    /**
     * Returns true when the node is leader.
     * @param blocking if true, will be blocked until the node finish it's state change
     */
    boolean isLeader(final boolean blocking);

    /**
     * Shutdown local replica node.
     *
     * @param done callback
     */
    void shutdown(final Closure done);

    /**
     * Block the thread until the node is successfully stopped.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    void join() throws InterruptedException;

    /**
     * [Thread-safe and wait-free]
     *
     * Apply task to the replicated-state-machine
     *
     * About the ownership:
     * |task.data|: for the performance consideration, we will take away the
     *               content. If you want keep the content, copy it before call
     *               this function
     * |task.done|: If the data is successfully committed to the raft group. We
     *              will pass the ownership to #{@link StateMachine#onApply(Iterator)}.
     *              Otherwise we will specify the error and call it.
     *
     * @param task task to apply
     */
    void apply(final Task task);

    /**
     * [Thread-safe and wait-free]
     *
     * Starts a linearizable read-only query request with request context(optional,
     * such as request id etc.) and closure.
     * The default value of ReadOnlyOption is {@link ReadOnlyOption#ReadOnlySafe}.
     * The closure will be called when the request is completed, and user can read
     * data from state machine if the result status is OK.
     *
     * @param requestContext the context of request
     * @param done           callback
     *
     * @since 0.0.3
     */
    void readIndex(final byte[] requestContext, final ReadIndexClosure done);

    /**
     * [Thread-safe and wait-free]
     *
     * Specify the ReadOnlyOption to execute a linearizable read-only query request.
     *
     * @param readOnlyOptions how the read only request is processed
     * @param requestContext the context of request
     * @param done           callback
     *
     * @since 1.3.13
     */
    void readIndex(final ReadOnlyOption readOnlyOptions, final byte[] requestContext, final ReadIndexClosure done);

    /**
     * List peers of this raft group, only leader returns.
     *
     * [NOTE] <strong>when list_peers concurrency with {@link #addPeer(PeerId, Closure)}/{@link #removePeer(PeerId, Closure)},
     * maybe return peers is staled.  Because {@link #addPeer(PeerId, Closure)}/{@link #removePeer(PeerId, Closure)}
     * immediately modify configuration in memory</strong>
     *
     * @return the peer list
     */
    List<PeerId> listPeers();

    /**
     * List all alive peers of this raft group, only leader returns.</p>
     *
     * [NOTE] <strong>list_alive_peers is just a transient data (snapshot)
     * and a short-term loss of response by the follower will cause it to
     * temporarily not exist in this list.</strong>
     *
     * @return the alive peer list
     * @since 1.2.6
     */
    List<PeerId> listAlivePeers();

    /**
     * List all learners of this raft group, only leader returns.</p>
     *
     * [NOTE] <strong>when listLearners concurrency with {@link #addLearners(List, Closure)}/{@link #removeLearners(List, Closure)}/{@link #resetLearners(List, Closure)},
     * maybe return peers is staled.  Because {@link #addLearners(List, Closure)}/{@link #removeLearners(List, Closure)}/{@link #resetLearners(List, Closure)}
     * immediately modify configuration in memory</strong>
     *
     * @return the learners set
     * @since 1.3.0
     */
    List<PeerId> listLearners();

    /**
     * List all alive learners of this raft group, only leader returns.</p>
     *
     * [NOTE] <strong>when listAliveLearners concurrency with {@link #addLearners(List, Closure)}/{@link #removeLearners(List, Closure)}/{@link #resetLearners(List, Closure)},
     * maybe return peers is staled.  Because {@link #addLearners(List, Closure)}/{@link #removeLearners(List, Closure)}/{@link #resetLearners(List, Closure)}
     * immediately modify configuration in memory</strong>
     *
     * @return the  alive learners set
     * @since 1.3.0
     */
    List<PeerId> listAliveLearners();

    /**
     * Add a new peer to the raft group. done.run() would be invoked after this
     * operation finishes, describing the detailed result.
     *
     * @param peer peer to add
     * @param done callback
     */
    void addPeer(final PeerId peer, final Closure done);

    /**
     * Remove the peer from the raft group. done.run() would be invoked after
     * operation finishes, describing the detailed result.
     *
     * @param peer peer to remove
     * @param done callback
     */
    void removePeer(final PeerId peer, final Closure done);

    /**
     * Change the configuration of the raft group to |newPeers| , done.run()
     * would be invoked after this operation finishes, describing the detailed result.
     *
     * @param newPeers new peers to change
     * @param done     callback
     */
    void changePeers(final Configuration newPeers, final Closure done);

    /**
     * Reset the configuration of this node individually, without any replication
     * to other peers before this node becomes the leader. This function is
     * supposed to be invoked when the majority of the replication group are
     * dead and you'd like to revive the service in the consideration of
     * availability.
     * Notice that neither consistency nor consensus are guaranteed in this
     * case, BE CAREFUL when dealing with this method.
     *
     * @param newPeers new peers
     */
    Status resetPeers(final Configuration newPeers);

    /**
     * Add some new learners to the raft group. done.run() will be invoked after this
     * operation finishes, describing the detailed result.
     *
     * @param learners learners to add
     * @param done     callback
     * @since 1.3.0
     */
    void addLearners(final List<PeerId> learners, final Closure done);

    /**
     * Remove some learners from the raft group. done.run() will be invoked after this
     * operation finishes, describing the detailed result.
     *
     * @param learners learners to remove
     * @param done     callback
     * @since 1.3.0
     */
    void removeLearners(final List<PeerId> learners, final Closure done);

    /**
     * Reset learners in the raft group. done.run() will be invoked after this
     * operation finishes, describing the detailed result.
     *
     * @param learners learners to set
     * @param done     callback
     * @since 1.3.0
     */
    void resetLearners(final List<PeerId> learners, final Closure done);

    /**
     * Start a snapshot immediately if possible. done.run() would be invoked when
     * the snapshot finishes, describing the detailed result.
     *
     * @param done callback
     */
    void snapshot(final Closure done);

    /**
     * Start to snapshot StateMachine immediately with the latest log applied to state machine.
     * You MUST call this method in {@link StateMachine} callback methods to trigger a snapshot synchronously, otherwise throws {@link IllegalStateException}.
     * And done.run() would be invoked when the snapshot finishes, describing the detailed result.
     *
     * @param done snapshot callback
     * @since 1.3.11
     * @throws IllegalStateException the method is not called in {@link StateMachine} callback methods.
     */
    void snapshotSync(final Closure done);

    /**
     * Reset the election_timeout for the every node.
     *
     * @param electionTimeoutMs the timeout millis of election
     */
    void resetElectionTimeoutMs(final int electionTimeoutMs);

    /**
     * Try transferring leadership to |peer|. If peer is ANY_PEER, a proper follower
     * will be chosen as the leader for the next term.
     * Returns 0 on success, -1 otherwise.
     *
     * @param peer the target peer of new leader
     * @return operation status
     */
    Status transferLeadershipTo(final PeerId peer);

    /**
     * Read the first committed user log from the given index.
     *   Return OK on success and user_log is assigned with the very data. Be awared
     *   that the user_log may be not the exact log at the given index, but the
     *   first available user log from the given index to lastCommittedIndex.
     *   Otherwise, appropriate errors are returned:
     *        - return ELOGDELETED when the log has been deleted;
     *        - return ENOMOREUSERLOG when we can't get a user log even reaching lastCommittedIndex.
     * [NOTE] in consideration of safety, we use lastAppliedIndex instead of lastCommittedIndex
     * in code implementation.
     *
     * @param index log index
     * @return user log entry
     * @throws LogNotFoundException  the user log is deleted at index.
     * @throws LogIndexOutOfBoundsException  the special index is out of bounds.
     */
    UserLog readCommittedUserLog(final long index);

    /**
     * SOFAJRaft users can implement the ReplicatorStateListener interface by themselves.
     * So users can do their own logical operator in this listener when replicator created, destroyed or had some errors.
     *
     * @param replicatorStateListener added ReplicatorStateListener which is implemented by users.
     */
    void addReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener);

    /**
     * End User can remove their implement the ReplicatorStateListener interface by themselves.
     *
     * @param replicatorStateListener need to remove the ReplicatorStateListener which has been added by users.
     */
    void removeReplicatorStateListener(final Replicator.ReplicatorStateListener replicatorStateListener);

    /**
     * Remove all the ReplicatorStateListeners which have been added by users.
     *
     */
    void clearReplicatorStateListeners();

    /**
     * Get the ReplicatorStateListeners which have been added by users.
     *
     * @return node's replicatorStatueListeners which have been added by users.
     */
    List<Replicator.ReplicatorStateListener> getReplicatorStatueListeners();

    /**
     * Get the node's target election priority value.
     *
     * @return node's target election priority value.
     * @since 1.3.0
     */
    int getNodeTargetPriority();

    /**
     * Get the node's state.
     *
     * @return node's state.
     * @since 1.3.8
     */
    State getNodeState();

    /**
     * Retrieve the last log index in log storage. Note: the last log may not be committed to raft group, and may be truncated in some cases.
     * @return the last log index
     * @since 1.3.12
     */
    long getLastLogIndex();

    /**
     * Retrieve the last log index that committed to the raft group.
     * @return the last committed log index
     * @since 1.3.12
     */
    long getLastCommittedIndex();

    /**
     * Retrieve the last log index that applied to state machine.
     * @return the last applied log index
     * @since 1.3.12
     */
    long getLastAppliedLogIndex();
}
