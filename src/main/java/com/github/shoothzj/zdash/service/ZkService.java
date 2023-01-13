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
import com.github.shoothzj.zdash.module.DeleteNodeReq;
import com.github.shoothzj.zdash.module.pulsar.TopicStats;
import com.github.shoothzj.zdash.util.JacksonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;

@Service
@Slf4j
public class ZkService {

    private final ZooKeeperConfig config;

    public ZkService(@Autowired ZooKeeperConfig config) {
        this.config = config;
    }

    public ZooKeeper newZookeeper() throws IOException {
        return new ZooKeeper(config.addr, config.sessionTimeoutMs, null);
    }

    public void putZnodeContent(String path, byte[] content, boolean createIfNotExists) throws Exception {
        try (ZooKeeper zooKeeper = new ZooKeeper(config.addr, config.sessionTimeoutMs, null)) {
            if (zooKeeper.exists(path, false) == null) {
                if (createIfNotExists) {
                    zooKeeper.create(path, content, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } else {
                    throw new RuntimeException("path not exists");
                }
            } else {
                zooKeeper.setData(path, content, -1);
            }
        }
    }

    public byte[] getZnodeContent(String path) throws Exception {
        try (ZooKeeper zooKeeper = new ZooKeeper(config.addr, config.sessionTimeoutMs, null)) {
            return zooKeeper.getData(path, false, new Stat());
        }
    }

    public byte[] getZnodeContent(ZooKeeper zooKeeper, String path) throws Exception {
        return zooKeeper.getData(path, false, new Stat());
    }

    public List<String> getChildren(String path) throws Exception {
        try (ZooKeeper zooKeeper = new ZooKeeper(config.addr, config.sessionTimeoutMs, null)) {
            return zooKeeper.getChildren(path, false);
        }
    }

    public List<String> getChildren(ZooKeeper zooKeeper, String path) throws Exception {
        return zooKeeper.getChildren(path, false);
    }

    public void deleteNode(DeleteNodeReq req) throws Exception {
        try (ZooKeeper zk = new ZooKeeper(config.addr, config.sessionTimeoutMs,
                watchedEvent -> log.info("zk process : {}", watchedEvent))
        ) {
            zk.delete(req.getPath(), req.getVersion());
        }
    }

    public List<String> getZnodesRecursive(String rootPath) throws Exception {
        try (ZooKeeper zk = new ZooKeeper(config.addr, config.sessionTimeoutMs,
                watchedEvent -> log.info("zk process : {}", watchedEvent))
        ) {
            final Deque<String> stack = new ArrayDeque<>();
            List<String> children = zk.getChildren(rootPath, null);
            if ("/".equals(rootPath)) {
                for (String child : children) {
                    stack.push(rootPath + child);
                }
            } else {
                for (String child : children) {
                    stack.push(rootPath + "/" + child);
                }
            }

            String path = "";
            List<String> znodes = new ArrayList<>();
            while ((path = stack.pollFirst()) != null) {
                znodes.add(path);
                List<String> childrens = zk.getChildren(path, null);
                for (String child : childrens) {
                    stack.push(path + "/" + child);
                }
            }
            return znodes;
        }
    }


    public HashMap<String, List<String>> getManagedLedgerTopics(ZooKeeper zooKeeper) throws Exception {
        final String tenantParentPath = "/managed-ledgers";
        List<String> tenants = getChildren(zooKeeper, tenantParentPath);
        // key: tenant_namespace_topic: value: partition collection
        HashMap<String, List<String>> partitionStatsMap = new HashMap<>();
        for (String tenant : tenants) {
            String namespaceParentPath = tenantParentPath + "/" + tenant;
            List<String> namespaces = getChildren(zooKeeper, namespaceParentPath);
            for (String namespace : namespaces) {
                try {
                    String tenantAndNamespace = tenant + "_" + namespace;
                    String partitionParentPath = namespaceParentPath + "/" + namespace + "/persistent";
                    List<String> partitions = getChildren(zooKeeper, partitionParentPath);
                    for (String partition : partitions) {
                        String[] arr = partition.split("-partition-");
                        if (arr.length != 2) {
                            log.warn("tenant: {}, namespace: {}, get patrition: {}", tenant, namespace, partition);
                            continue;
                        }
                        String topicPath = tenantAndNamespace + "_" + arr[0];
                        List<String> partitionList = partitionStatsMap.get(topicPath);
                        if (partitionList == null) {
                            partitionList = new ArrayList<>();
                        }
                        partitionList.add(arr[1]);
                        partitionStatsMap.put(topicPath, partitionList);
                    }
                } catch (KeeperException.NoNodeException e) {
                    log.warn("tenant: {}, namespace: {}, not partition topic", tenant, namespace);
                }
            }
        }
        return partitionStatsMap;
    }

    public HashMap<String, Integer> getAdminPartitionTopics(ZooKeeper zooKeeper) throws Exception {
        final String adminTenantParentPath = "/admin/partitioned-topics";
        List<String> adminTenants = getChildren(zooKeeper, adminTenantParentPath);
        HashMap<String, Integer> partitionStat = new HashMap<>();
        for (String tenant : adminTenants) {
            String namespaceParentPath = adminTenantParentPath + "/" + tenant;
            List<String> namespaces = getChildren(zooKeeper, namespaceParentPath);
            for (String namespace : namespaces) {
                try {
                    String topicParentPath = namespaceParentPath + "/" + namespace + "/persistent";
                    List<String> topics = getChildren(zooKeeper, topicParentPath);
                    // get topic node content
                    for (String topic : topics) {
                        String topicPath = topicParentPath + "/" + topic;
                        byte[] rawBody = getZnodeContent(zooKeeper, topicPath);
                        String content = new String(rawBody, StandardCharsets.UTF_8);
                        TopicStats info = JacksonUtil.toObject(content, TopicStats.class);
                        if (info == null) {
                            log.warn("unmarshal admin partition-topic data failed, content: {}", content);
                            continue;
                        }
                        partitionStat.put(tenant + "_" + namespace + "_" + topic, info.getPartitions());
                    }
                } catch (KeeperException.NoNodeException e) {
                    log.warn("tenant: {}, namespace: {}, not partition topic", tenant, namespace);
                }
            }
        }
        return partitionStat;
    }
}
