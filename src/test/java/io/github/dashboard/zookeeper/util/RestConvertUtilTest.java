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

import io.github.dashboard.zookeeper.TestData;
import io.github.dashboard.zookeeper.module.pulsar.SchemaLocator;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.protocol.pulsar.codec.schema.SchemaStorageFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RestConvertUtilTest {

    @Test
    public void testConvertSchemaLocator() throws InvalidProtocolBufferException {
        byte[] data = HexUtil.hexToByteArray(TestData.EXAMPLE_PULSAR_SCHEMA_LOCATOR);
        SchemaStorageFormat.SchemaLocator schemaLocator = DecodeUtil.decodePulsarSchemaLocator(data);
        SchemaLocator resp = RestConvertUtil.convert(schemaLocator);
        Assertions.assertNotNull(resp);
    }

}
