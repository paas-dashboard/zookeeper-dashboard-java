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

import com.github.shoothzj.zdash.module.DecodeComponent;
import com.github.shoothzj.zdash.module.DecodeNamespace;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.broker.service.schema.SchemaStorageFormat;

import java.nio.charset.StandardCharsets;

public class DecodeUtil {

    public static String decodeData(byte[] data, String component, String namespace) throws Exception {
        DecodeComponent decodeComponent = DecodeComponent.valueOf(component);
        switch (decodeComponent) {
            case Pulsar:
                return decodePulsarData(data, namespace);
            default:
                return new String(data, StandardCharsets.UTF_8);
        }
    }

    public static String decodePulsarData(byte[] data, String namespace) throws Exception {
        DecodeNamespace decodeNamespace = DecodeNamespace.valueOf(namespace);
        switch (decodeNamespace) {
            case ManagedLedgerTopic:
                return decodePulsarManagedLedgerTopicData(data).toString();
            case ManagedLedgerSubscription:
                return decodePulsarManagedLedgerSubscriptionData(data).toString();
            case SchemaLocator:
                return decodePulsarSchemaLocator(data).toString();
            default:
                return new String(data, StandardCharsets.UTF_8);
        }
    }

    public static MLDataFormats.ManagedLedgerInfo decodePulsarManagedLedgerTopicData(byte[] data)
            throws InvalidProtocolBufferException {
        return MLDataFormats.ManagedLedgerInfo.parseFrom(data);
    }

    public static MLDataFormats.ManagedCursorInfo decodePulsarManagedLedgerSubscriptionData(byte[] data)
            throws InvalidProtocolBufferException {
        return MLDataFormats.ManagedCursorInfo.parseFrom(data);
    }

    public static SchemaStorageFormat.SchemaLocator decodePulsarSchemaLocator(byte[] data)
            throws InvalidProtocolBufferException {
        return SchemaStorageFormat.SchemaLocator.parseFrom(data);
    }

}
