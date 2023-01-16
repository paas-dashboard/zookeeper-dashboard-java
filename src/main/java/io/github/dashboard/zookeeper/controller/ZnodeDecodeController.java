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

import io.github.dashboard.zookeeper.module.DecodeComponent;
import io.github.dashboard.zookeeper.module.DecodeNamespace;
import io.github.dashboard.zookeeper.module.SupportDecodeComponentListResp;
import io.github.dashboard.zookeeper.module.SupportDecodeNamespaceListResp;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/zookeeper")
public class ZnodeDecodeController {

    @GetMapping("/decode-components")
    public ResponseEntity<SupportDecodeComponentListResp> getDecodeComponents() {
        List<String> decodeComponents = new ArrayList<>();
        for (DecodeComponent decodeComponent : DecodeComponent.values()) {
            decodeComponents.add(decodeComponent.toString());
        }
        SupportDecodeComponentListResp componentListResp = new SupportDecodeComponentListResp();
        componentListResp.setSupportDecodeComponents(decodeComponents);
        return new ResponseEntity<>(componentListResp, HttpStatus.OK);
    }

    @GetMapping("/decode-namespaces")
    public ResponseEntity<SupportDecodeNamespaceListResp> getDecodeNamespaces() {
        List<String> decodeNamespaces = new ArrayList<>();
        for (DecodeNamespace decodeNamespace : DecodeNamespace.values()) {
            decodeNamespaces.add(decodeNamespace.toString());
        }
        SupportDecodeNamespaceListResp namespaceListResp = new SupportDecodeNamespaceListResp();
        namespaceListResp.setSupportDecodeNamespaces(decodeNamespaces);
        return new ResponseEntity<>(namespaceListResp, HttpStatus.OK);
    }
}
