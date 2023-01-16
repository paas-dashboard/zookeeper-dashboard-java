package com.github.shoothzj.zdash.controller;

import com.github.shoothzj.zdash.module.GetNodeReq;
import com.github.shoothzj.zdash.module.GetNodeResp;
import com.github.shoothzj.zdash.module.pulsar.SchemaLocator;
import com.github.shoothzj.zdash.service.ZkService;
import com.github.shoothzj.zdash.util.DecodeUtil;
import com.github.shoothzj.zdash.util.RestConvertUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.schema.SchemaStorageFormat;
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
                                              String namespace) {
        log.info("decode node path [{}]", req.getPath());
        try {
            byte[] data = zkService.getZnodeContent(req.getPath());
            GetNodeResp dataResp = new GetNodeResp();
            dataResp.setData(DecodeUtil.decodeData(data, component, namespace));
            return new ResponseEntity<>(dataResp, HttpStatus.OK);
        } catch (Exception e) {
            log.error("get node fail. err: ", e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/get-node-decode-pulsar-schema-locator")
    public ResponseEntity<SchemaLocator> decodePulsarSchemaLocator(@RequestBody GetNodeReq req) {
        log.info("decode node path [{}]", req.getPath());
        try {
            byte[] data = zkService.getZnodeContent(req.getPath());
            SchemaStorageFormat.SchemaLocator schemaLocator = DecodeUtil.decodePulsarSchemaLocator(data);
            return new ResponseEntity<>(RestConvertUtil.convert(schemaLocator), HttpStatus.OK);
        } catch (Exception e) {
            log.error("get node fail. err: ", e);
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}