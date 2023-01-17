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

package io.github.dashboard.zookeeper.util;

import io.github.dashboard.zookeeper.module.pulsar.IndexEntry;
import io.github.dashboard.zookeeper.module.pulsar.LedgerInfo;
import io.github.dashboard.zookeeper.module.pulsar.ManagedCursor;
import io.github.dashboard.zookeeper.module.pulsar.ManagedLedger;
import io.github.dashboard.zookeeper.module.pulsar.PositionInfo;
import io.github.dashboard.zookeeper.module.pulsar.SchemaLocator;
import io.github.protocol.pulsar.codec.mledger.MLDataFormats;
import io.github.protocol.pulsar.codec.schema.SchemaStorageFormat;

import java.util.ArrayList;
import java.util.List;

public class RestConvertUtil {

    public static SchemaLocator convert(SchemaStorageFormat.SchemaLocator schemaLocator) {
        SchemaLocator result = new SchemaLocator();
        result.setInfo(convert(schemaLocator.getInfo()));
        result.setIndex(convert(schemaLocator.getIndexList()));
        return result;
    }

    public static List<IndexEntry> convert(List<SchemaStorageFormat.IndexEntry> indexEntries) {
        List<IndexEntry> list = new ArrayList<>();
        for (SchemaStorageFormat.IndexEntry entry : indexEntries) {
            list.add(convert(entry));
        }
        return list;
    }

    public static IndexEntry convert(SchemaStorageFormat.IndexEntry indexEntry) {
        IndexEntry result = new IndexEntry();
        result.setPosition(convert(indexEntry.getPosition()));
        return result;
    }

    public static PositionInfo convert(SchemaStorageFormat.PositionInfo positionInfo) {
        PositionInfo result = new PositionInfo();
        result.setLedgerId(positionInfo.getLedgerId());
        result.setEntryId(positionInfo.getEntryId());
        return result;
    }

    public static ManagedLedger convert(MLDataFormats.ManagedLedgerInfo managedLedgerInfo) {
        ManagedLedger result = new ManagedLedger();
        List<LedgerInfo> ledgers = new ArrayList<>();
        managedLedgerInfo.getLedgerInfoList().forEach(ledgerInfo -> {
            ledgers.add(convert(ledgerInfo));
        });
        result.setLedgers(ledgers);
        return result;
    }

    public static ManagedCursor convert(MLDataFormats.ManagedCursorInfo managedCursorInfo) {
        ManagedCursor result = new ManagedCursor();
        result.setCursorLedgerId(managedCursorInfo.getCursorsLedgerId());
        result.setMarkDeleteLedgerId(managedCursorInfo.getMarkDeleteLedgerId());
        result.setMarkDeleteEntryId(result.getMarkDeleteEntryId());
        return result;
    }

    public static LedgerInfo convert(MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo) {
        LedgerInfo result = new LedgerInfo();
        result.setLedgerId(ledgerInfo.getLedgerId());
        result.setEntries(ledgerInfo.getEntries());
        result.setSize(ledgerInfo.getSize());
        return result;
    }

}
