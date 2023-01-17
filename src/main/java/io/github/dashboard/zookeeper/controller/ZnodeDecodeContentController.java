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

import io.github.dashboard.zookeeper.module.GetNodeReq;
import io.github.dashboard.zookeeper.module.GetNodeResp;
import io.github.dashboard.zookeeper.module.pulsar.ManagedCursor;
import io.github.dashboard.zookeeper.module.pulsar.ManagedLedger;
import io.github.dashboard.zookeeper.module.pulsar.SchemaLocator;
import io.github.dashboard.zookeeper.service.ZkService;
import io.github.dashboard.zookeeper.util.DecodeUtil;
import io.github.dashboard.zookeeper.util.RestConvertUtil;
import io.github.protocol.pulsar.codec.mledger.MLDataFormats;
import io.github.protocol.pulsar.codec.schema.SchemaStorageFormat;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/zookeeper")
public class ZnodeDecodeContentController {

    @Autowired
    private ZkService zkService;

    @PostMapping("/get-node-decode")
    public ResponseEntity<GetNodeResp> decode(@RequestBody GetNodeReq req,
                                              @RequestParam(value = "decodeComponent", required = false)
                                              String component,
                                              @RequestParam(value = "decodeNamespace", required = false)
                                              String namespace) throws Exception {
        log.info("decode node path [{}]", req.getPath());
        byte[] data = zkService.getZnodeContent(req.getPath());
        GetNodeResp dataResp = new GetNodeResp();
        dataResp.setData(DecodeUtil.decodeData(data, component, namespace));
        return new ResponseEntity<>(dataResp, HttpStatus.OK);
    }

    @PostMapping("/get-node-decode-pulsar-schema-locator")
    public ResponseEntity<SchemaLocator> decodePulsarSchemaLocator(@RequestBody GetNodeReq req)
            throws Exception {
        log.info("decode node path [{}]", req.getPath());
        byte[] data = zkService.getZnodeContent(req.getPath());
        SchemaStorageFormat.SchemaLocator schemaLocator = DecodeUtil.decodePulsarSchemaLocator(data);
        return new ResponseEntity<>(RestConvertUtil.convert(schemaLocator), HttpStatus.OK);
    }

    @PostMapping("/get-node-decode-pulsar-managed-ledger")
    public ResponseEntity<ManagedLedger> decodePulsarManagedLedger(@RequestBody GetNodeReq req)
            throws Exception {
        log.info("decode node path [{}]", req.getPath());
        byte[] data = zkService.getZnodeContent(req.getPath());
        MLDataFormats.ManagedLedgerInfo managedLedgerInfo = DecodeUtil.decodePulsarMangedLedger(data);
        return new ResponseEntity<>(RestConvertUtil.convert(managedLedgerInfo), HttpStatus.OK);
    }

    @PostMapping("/get-node-decode-pulsar-managed-cursor")
    public ResponseEntity<ManagedCursor> decodePulsarManagedCursor(@RequestBody GetNodeReq req)
            throws Exception {
        log.info("decode node path [{}]", req.getPath());
        byte[] data = zkService.getZnodeContent(req.getPath());
        MLDataFormats.ManagedCursorInfo cursor = DecodeUtil.decodePulsarManagedCursor(data);
        return new ResponseEntity<>(RestConvertUtil.convert(cursor), HttpStatus.OK);
    }
}
