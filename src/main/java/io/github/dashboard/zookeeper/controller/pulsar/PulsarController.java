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

import io.github.dashboard.zookeeper.service.BookkeeperService;
import io.github.dashboard.zookeeper.service.PulsarService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/zookeeper/pulsar")
public class PulsarController {

    @Autowired
    private BookkeeperService bookkeeperService;

    @Autowired
    private PulsarService pulsarService;

    @GetMapping("/zombie-ledgers")
    public ResponseEntity<List<Long>> getZombieLedgers() throws Exception {
        List<Long> zombies = new ArrayList<>();
        List<Long> bkLedgers = bookkeeperService.getAllLedgers();
        log.info("get bookkeeper ledgers : {}", bkLedgers);
        List<Long> pulsarLedgers = pulsarService.getPulsarLedgers();
        log.info("get pulsar ledgers : {}", pulsarLedgers);
        bkLedgers.forEach(bkLedger -> {
            if (!pulsarLedgers.contains(bkLedger)) {
                zombies.add(bkLedger);
            }
        });
        log.info("get zombie ledgers : {}", zombies);
        return new ResponseEntity<>(zombies, HttpStatus.OK);
    }

}
