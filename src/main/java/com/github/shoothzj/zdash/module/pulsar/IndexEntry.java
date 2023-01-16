package com.github.shoothzj.zdash.module.pulsar;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class IndexEntry {

    private long version;

    private PositionInfo position;

    public IndexEntry() {
    }
}
