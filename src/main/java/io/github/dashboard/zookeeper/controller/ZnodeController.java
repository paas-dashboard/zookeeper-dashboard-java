/*
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

package io.github.dashboard.zookeeper.controller;

import io.github.dashboard.zookeeper.module.DeleteNodeReq;
import io.github.dashboard.zookeeper.module.GetNodeReq;
import io.github.dashboard.zookeeper.module.GetNodeResp;
import io.github.dashboard.zookeeper.module.GetNodesReq;
import io.github.dashboard.zookeeper.module.GetNodesResp;
import io.github.dashboard.zookeeper.module.SaveNodeReq;
import io.github.dashboard.zookeeper.module.pulsar.DiffPartitionResp;
import io.github.dashboard.zookeeper.service.ZkService;
import io.github.dashboard.zookeeper.util.HexUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/zookeeper")
public class ZnodeController {

    @Autowired
    private ZkService zkService;

    @PostMapping("/get-nodes")
    public ResponseEntity<GetNodesResp> getNodes(@RequestBody GetNodesReq req,
                                                 @RequestParam boolean recursive) {
        log.info("getNodes path [{}]", req.getPath());
        try {
            GetNodesResp getNodeResp = new GetNodesResp();
            if (recursive) {
                getNodeResp.setNodes(zkService.getZnodesRecursive(req.getPath()));
            } else {
                getNodeResp.setNodes(zkService.getChildren(req.getPath()));
            }
            return new ResponseEntity<>(getNodeResp, HttpStatus.OK);
        } catch (Exception e) {
            log.error("get nodes fail. err: ", e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PutMapping("/nodes")
    public ResponseEntity<Void> saveData(@RequestBody SaveNodeReq saveNodeReq) throws Exception {
        zkService.putZnodeContent(saveNodeReq.getPath(),
                saveNodeReq.getValue().getBytes(StandardCharsets.UTF_8), true);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @PostMapping("/nodes/delete-node")
    public ResponseEntity<Void> deleteNode(@RequestBody DeleteNodeReq req) throws Exception {
        zkService.deleteNode(req);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @PostMapping("/get-node")
    public ResponseEntity<GetNodeResp> getNode(@RequestBody GetNodeReq req,
                                               @RequestParam(value = "codec", required = false) String codec)
            throws Exception {
        log.info("getNode path [{}]", req.getPath());
        byte[] data = zkService.getZnodeContent(req.getPath());
        GetNodeResp dataResp = new GetNodeResp();
        if ("hex".equalsIgnoreCase(codec)) {
            dataResp.setData(HexUtil.bytes2hex(data));
        } else {
            dataResp.setData(new String(data, StandardCharsets.UTF_8));
        }
        return new ResponseEntity<>(dataResp, HttpStatus.OK);
    }

    @PostMapping("/check-pulsar-partition-topic-metadata")
    public ResponseEntity<DiffPartitionResp> checkPulsarPartitionTopicMetadata() throws Exception {
        DiffPartitionResp diffPartitionResp = new DiffPartitionResp();
        List<DiffPartitionResp.DiffPartition> diffPartition = new ArrayList<>();
        try (ZooKeeper zooKeeper = zkService.newZookeeper()) {
            // get all topic from managed-ledgers node, save to map, key tenant_namespace_topic, value partition size
            HashMap<String, List<String>> partitionStatsMap = zkService.getManagedLedgerTopics(zooKeeper);

            // get all topic from /admin/partitioned-topics node, save to map
            HashMap<String, Integer> partitionStat = zkService.getAdminPartitionTopics(zooKeeper);

            // diff topic
            for (Map.Entry<String, List<String>> entry : partitionStatsMap.entrySet()) {
                String ledgerTopic = entry.getKey();
                int ledgerPartitionSize = entry.getValue().size();
                Integer adminPartitionSize = partitionStat.get(ledgerTopic);
                if (adminPartitionSize == null) {
                    log.warn("different topic: {}", ledgerTopic);
                    String reason = String.format("admin partition topic size: 0, managed-ledger topic size: %d",
                            ledgerPartitionSize);
                    List<String> partitions = new ArrayList<>();
                    for (String p : entry.getValue()) {
                        partitions.add(ledgerTopic + "-partition-" + p);
                    }
                    diffPartition.add(new DiffPartitionResp.DiffPartition(ledgerTopic, partitions, reason));
                    continue;
                }
                if (ledgerPartitionSize != adminPartitionSize) {
                    log.warn("different topic: {}", ledgerTopic);
                    String reason = String.format("admin partition topic size: %d, managed-ledger topic size: %d",
                            adminPartitionSize, ledgerPartitionSize);
                    List<String> partitions = new ArrayList<>();
                    for (String p : entry.getValue()) {
                        partitions.add(ledgerTopic + "-partition-" + p);
                    }
                    diffPartition.add(new DiffPartitionResp.DiffPartition(ledgerTopic, partitions, reason));
                }
            }
            diffPartitionResp.setDiffs(diffPartition);
        }

        return new ResponseEntity<>(diffPartitionResp, HttpStatus.OK);
    }

}
