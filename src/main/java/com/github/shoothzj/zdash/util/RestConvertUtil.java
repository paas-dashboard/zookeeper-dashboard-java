package com.github.shoothzj.zdash.util;

import com.github.shoothzj.zdash.module.pulsar.IndexEntry;
import com.github.shoothzj.zdash.module.pulsar.PositionInfo;
import com.github.shoothzj.zdash.module.pulsar.SchemaLocator;
import org.apache.pulsar.broker.service.schema.SchemaStorageFormat;

import java.util.ArrayList;
import java.util.List;

public class RestConvertUtil {

    public static SchemaLocator convert(SchemaStorageFormat.SchemaLocator schemaLocator) {
        SchemaLocator result = new SchemaLocator();
        result.setInfo(convert(schemaLocator.getInfo()));
        result.setIndex(convert(schemaLocator.getIndexList()));
        return result;
    }

    public static List<IndexEntry> convert(List<SchemaStorageFormat.IndexEntry> indexEntries) {
        List<IndexEntry> list = new ArrayList<>();
        for (SchemaStorageFormat.IndexEntry entry : indexEntries) {
            list.add(convert(entry));
        }
        return list;
    }

    public static IndexEntry convert(SchemaStorageFormat.IndexEntry indexEntry) {
        IndexEntry result = new IndexEntry();
        result.setPosition(convert(indexEntry.getPosition()));
        return result;
    }

    public static PositionInfo convert(SchemaStorageFormat.PositionInfo positionInfo) {
        PositionInfo result = new PositionInfo();
        result.setLedgerId(positionInfo.getLedgerId());
        result.setEntryId(positionInfo.getEntryId());
        return result;
    }

}
