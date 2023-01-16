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

package com.github.shoothzj.zdash.util;

import com.github.shoothzj.zdash.module.pulsar.IndexEntry;
import com.github.shoothzj.zdash.module.pulsar.PositionInfo;
import com.github.shoothzj.zdash.module.pulsar.SchemaLocator;
import org.apache.pulsar.broker.service.schema.SchemaStorageFormat;

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

}
