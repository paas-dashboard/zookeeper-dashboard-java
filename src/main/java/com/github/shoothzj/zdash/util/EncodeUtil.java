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

package com.github.shoothzj.zdash.util;

import com.github.shoothzj.zdash.module.pulsar.LedgerInfoReq;
import com.github.shoothzj.zdash.module.pulsar.ManagedLedgerTopicReq;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;

public class EncodeUtil {

    public static byte[] encodePulsarManagedLedgerTopic(ManagedLedgerTopicReq req) {
        MLDataFormats.ManagedLedgerInfo.Builder builder = MLDataFormats.ManagedLedgerInfo.newBuilder();
        for (LedgerInfoReq ledgerInfoReq : req.getLedgerInfoReqList()) {
            MLDataFormats.ManagedLedgerInfo.LedgerInfo.Builder ledgerInfoBuilder = MLDataFormats.
                    ManagedLedgerInfo.LedgerInfo.newBuilder();
            ledgerInfoBuilder.setLedgerId(ledgerInfoReq.getLedgerId());
            if (ledgerInfoReq.getTimestamp().isPresent()) {
                ledgerInfoBuilder.setTimestamp(ledgerInfoReq.getTimestamp().get());
            }
            builder.addLedgerInfo(ledgerInfoBuilder);
        }
        return builder.build().toByteArray();
    }

}
