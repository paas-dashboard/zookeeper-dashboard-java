/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.shoothzj.zdash.service;

import com.github.shoothzj.zdash.config.ZooKeeperConfig;
import io.github.embedded.zookeeper.core.EmbeddedZkServer;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

class ZkServiceTest {

    private static ZkService zkService;

    @BeforeAll
    public static void beforeAll() throws Exception {
        EmbeddedZkServer embeddedZkServer = new EmbeddedZkServer();
        embeddedZkServer.start();
        ZooKeeperConfig zooKeeperConfig = new ZooKeeperConfig();
        zooKeeperConfig.addr = embeddedZkServer.getZkAddr();
        zooKeeperConfig.sessionTimeoutMs = 15_000;
        zkService = new ZkService(zooKeeperConfig);
    }

    @Test
    void testZkGetNodeContent() throws Exception {
        byte[] content = zkService.getZnodeContent("/zookeeper");
        Assertions.assertNotNull(content);
    }

    @Test
    void testCheckPulsarPartitionTopicMetadata() throws Exception {
        String ledgerTopicPath = "/managed-ledgers/public/default/persistent";
        zkService.putZnodeContent("/managed-ledgers", null, true);
        zkService.putZnodeContent("/managed-ledgers/public", null, true);
        zkService.putZnodeContent("/managed-ledgers/public/default", null, true);
        zkService.putZnodeContent(ledgerTopicPath, null, true);
        zkService.putZnodeContent(ledgerTopicPath + "/topic1-partition-0", null, true);
        zkService.putZnodeContent(ledgerTopicPath + "/topic2-partition-0", null, true);
        zkService.putZnodeContent(ledgerTopicPath + "/topic2-partition-1", null, true);

        String partitionTopicPath = "/admin/partitioned-topics/public/default/persistent";
        zkService.putZnodeContent("/admin", null, true);
        zkService.putZnodeContent("/admin/partitioned-topics", null, true);
        zkService.putZnodeContent("/admin/partitioned-topics/public", null, true);
        zkService.putZnodeContent("/admin/partitioned-topics/public/default", null, true);
        zkService.putZnodeContent(partitionTopicPath, null, true);
        zkService.putZnodeContent(partitionTopicPath + "/topic1", "{\"partitions\":1}".getBytes(), true);
        zkService.putZnodeContent(partitionTopicPath + "/topic2", "{\"partitions\":3}".getBytes(), true);

        ZooKeeper zooKeeper = zkService.newZookeeper();

        HashMap<String, List<String>> partitionStatsMap = zkService.getManagedLedgerTopics(zooKeeper);
        List<String> topics1 = partitionStatsMap.get("public_default_topic1");
        Assertions.assertEquals(1, topics1.size());
        List<String> topics2 = partitionStatsMap.get("public_default_topic2");
        Assertions.assertEquals(2, topics2.size());


        HashMap<String, Integer> partitionStat = zkService.getAdminPartitionTopics(zooKeeper);
        Integer partition1 = partitionStat.get("public_default_topic1");
        Assertions.assertEquals(1, partition1);
        Integer partition2 = partitionStat.get("public_default_topic2");
        Assertions.assertEquals(3, partition2);

        Integer noExistPartition = partitionStat.get("public_default_no_exist");
        Assertions.assertNull(noExistPartition);
    }

}
