package com.github.shoothzj.zdash.controller;

import com.github.shoothzj.zdash.module.DecodeComponent;
import com.github.shoothzj.zdash.module.DecodeNamespace;
import com.github.shoothzj.zdash.module.SupportDecodeComponentListResp;
import com.github.shoothzj.zdash.module.SupportDecodeNamespaceListResp;
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
