package com.github.shoothzj.zdash.module.pulsar;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class PositionInfo {

    private long ledgerId;

    private long entryId;

    public PositionInfo() {
    }
}
