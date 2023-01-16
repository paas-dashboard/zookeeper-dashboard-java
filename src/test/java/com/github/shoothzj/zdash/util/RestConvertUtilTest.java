package com.github.shoothzj.zdash.util;

import com.github.shoothzj.zdash.TestData;
import com.github.shoothzj.zdash.module.pulsar.SchemaLocator;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.pulsar.broker.service.schema.SchemaStorageFormat;
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
