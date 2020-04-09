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
package com.alipay.sofa.jraft.example.priorityelection;


/**
 *
 * @author zongtanghu
 */
public class PriorityElectionBootstrap {

    // Start elections by 3 instance. Note that if multiple instances are started on the same machine,
    // the first parameter `dataPath` should not be the same.
    public static void main(final String[] args) {
        if (args.length < 4) {
            System.out
                    .println("Useage : java com.alipay.sofa.jraft.example.priorityelection.ElectionBootstrap {dataPath} {groupId} {serverId} {initConf}");
            System.out
                    .println("Example: java com.alipay.sofa.jraft.example.priorityelection.ElectionBootstrap /tmp/server1 election_test 127.0.0.1:8081::100 127.0.0.1:8081::100,127.0.0.1:8082::40,127.0.0.1:8083::40");
            System.exit(1);
        }
        final String dataPath = args[0];
        final String groupId = args[1];
        final String serverIdStr = args[2];
        final String initialConfStr = args[3];

        final PriorityElectionNodeOptions priorityElectionOpts = new PriorityElectionNodeOptions();
        priorityElectionOpts.setDataPath(dataPath);
        priorityElectionOpts.setGroupId(groupId);
        priorityElectionOpts.setServerAddress(serverIdStr);
        priorityElectionOpts.setInitialServerAddressList(initialConfStr);

        final PriorityElectionNode node = new PriorityElectionNode();
        node.addLeaderStateListener(new LeaderStateListener() {

            @Override
            public void onLeaderStart(long leaderTerm) {
                System.out.println("[PriorityElectionBootstrap] Leader start on term: " + leaderTerm);
            }

            @Override
            public void onLeaderStop(long leaderTerm) {
                System.out.println("[PriorityElectionBootstrap] Leader stop on term: " + leaderTerm);
            }
        });
        node.init(priorityElectionOpts);
    }

}
