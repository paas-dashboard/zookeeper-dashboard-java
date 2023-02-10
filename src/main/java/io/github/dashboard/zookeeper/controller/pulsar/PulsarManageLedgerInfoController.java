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

package io.github.dashboard.zookeeper.controller.pulsar;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.dashboard.zookeeper.module.pulsar.DeleteTopicLedgerReq;
import io.github.dashboard.zookeeper.module.UpdateInf;
import io.github.dashboard.zookeeper.service.ZkService;
import io.github.protocol.pulsar.codec.mledger.MLDataFormats;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/zookeeper/pulsar/manage-ledger-info")
public class PulsarManageLedgerInfoController {

    @Autowired
    private ZkService zkService;

    @PostMapping("/individual-ledger")
    public ResponseEntity<Void> deleteTopicLedger(@RequestBody DeleteTopicLedgerReq req)
            throws Exception {
        zkService.updateZnodeContentCas(req.getPath(), new UpdateInf<byte[]>() {
            @Override
            public byte[] update(byte[] bytes) {
                try {
                    MLDataFormats.ManagedLedgerInfo managedLedgerInfo =
                            MLDataFormats.ManagedLedgerInfo.parseFrom(bytes);
                    return cloneAndDeleteIndividualLedger(managedLedgerInfo, req.getLedgerId()).toByteArray();
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        return new ResponseEntity<>(HttpStatus.OK);
    }

    public static MLDataFormats.ManagedLedgerInfo cloneAndDeleteIndividualLedger
            (MLDataFormats.ManagedLedgerInfo managedLedgerInfo, long targetLedgerId) {
        List<MLDataFormats.ManagedLedgerInfo.LedgerInfo> ledgers = new ArrayList<>(
                managedLedgerInfo.getLedgerInfoList());
        Iterator<MLDataFormats.ManagedLedgerInfo.LedgerInfo> iterator = ledgers.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getLedgerId() == targetLedgerId) {
                iterator.remove();
                break;
            }
        }
        MLDataFormats.ManagedLedgerInfo.Builder builder = MLDataFormats.ManagedLedgerInfo.newBuilder()
                .addAllLedgerInfo(ledgers)
                .addAllProperties(managedLedgerInfo.getPropertiesList());
        builder.setTerminatedPosition(MLDataFormats.NestedPositionInfo.newBuilder()
                .setLedgerId(managedLedgerInfo.getTerminatedPosition().getLedgerId())
                .setEntryId(managedLedgerInfo.getTerminatedPosition().getEntryId()).build());
        return builder.build();
    }

}
