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

import io.github.dashboard.zookeeper.constant.BookkeeperConst;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class BookkeeperService {

    @Autowired
    private ZkService zkService;

    public List<Long> getAllLedgers() throws Exception {
        List<Long> ledgers = new ArrayList<>();
        try (ZooKeeper zooKeeper = zkService.newZookeeper()) {
            List<String> floor1 = zkService.getChildren(zooKeeper, BookkeeperConst.BOOKKEEPER_LEDGER_PATH);
            floor1.removeAll(List.of(
                    BookkeeperConst.BOOKKEEPER_COOKIES,
                    BookkeeperConst.BOOKKEEPER_AVAILABLE,
                    BookkeeperConst.BOOKKEEPER_IDGEN,
                    BookkeeperConst.BOOKKEEPER_LAYOUT,
                    BookkeeperConst.BOOKKEEPER_INSTANCE_ID,
                    BookkeeperConst.BOOKKEEPER_UNDER_REPLICATION));
            for (String ledgerRoot : floor1) {
                String absoluteLedgerRoot = BookkeeperConst.BOOKKEEPER_LEDGER_PATH + "/" + ledgerRoot;
                List<String> floor2 = zkService.getChildren(zooKeeper, absoluteLedgerRoot);
                for (String ledgerMiddle : floor2) {
                    String ledgerIdHeader = ledgerRoot + ledgerMiddle;
                    List<String> floor3 = zkService.getChildren(zooKeeper, absoluteLedgerRoot + "/" + ledgerMiddle);
                    for (String ledgerEnd : floor3) {
                        try {
                            ledgers.add(Long.parseLong(ledgerIdHeader + ledgerEnd.replace("L", "")));
                        } catch (Throwable e) {
                            // no nothing
                        }
                    }
                }
            }
        }
        return ledgers;
    }

}
