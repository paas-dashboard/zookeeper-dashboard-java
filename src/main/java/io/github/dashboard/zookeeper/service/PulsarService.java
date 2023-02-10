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

package io.github.dashboard.zookeeper.service;

import io.github.dashboard.zookeeper.constant.PulsarConst;
import io.github.dashboard.zookeeper.util.DecodeUtil;
import io.github.protocol.pulsar.codec.mledger.MLDataFormats;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class PulsarService {

    @Autowired
    private ZkService zkService;

    public List<MLDataFormats.ManagedLedgerInfo> getManagedLedgerTopics() throws Exception {
        List<MLDataFormats.ManagedLedgerInfo> ledgerInfos = new ArrayList<>();
        try (ZooKeeper zooKeeper = zkService.newZookeeper()) {
            List<String> tenants = zkService.getChildren(zooKeeper, PulsarConst.PULSAR_MANAGED_LEDGER_PATH);
            for (String tenant : tenants) {
                String tenantPath = PulsarConst.PULSAR_MANAGED_LEDGER_PATH + "/" + tenant;
                List<String> namespaces = zkService.getChildren(zooKeeper, tenantPath);
                for (String namespace : namespaces) {
                    String namespacePath = tenantPath + "/" + namespace;
                    List<String> topics = zkService.getChildren(zooKeeper, namespacePath);
                    for (String topic : topics) {
                        ledgerInfos.add(DecodeUtil.decodePulsarManagedLedgerTopicData(
                                zkService.getZnodeContent(zooKeeper, topic)));
                    }
                }
            }
        }
        return ledgerInfos;
    }

    public List<Long> getPulsarLedgers() throws Exception {
        List<Long> ledgers = new ArrayList<>();
        getManagedLedgerTopics().forEach(managedLedgerInfo ->
                managedLedgerInfo.getLedgerInfoList().forEach(ledgerInfo ->
                        ledgers.add(ledgerInfo.getLedgerId())));
        return ledgers;
    }

}
