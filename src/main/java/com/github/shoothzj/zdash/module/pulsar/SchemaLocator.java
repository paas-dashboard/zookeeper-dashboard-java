package com.github.shoothzj.zdash.module.pulsar;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class SchemaLocator {

    private IndexEntry info;

    private List<IndexEntry> index;

    public SchemaLocator() {
    }
}
