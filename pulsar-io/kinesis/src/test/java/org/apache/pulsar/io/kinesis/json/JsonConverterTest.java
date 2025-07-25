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
package org.apache.pulsar.io.kinesis.json;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.testng.annotations.Test;

public class JsonConverterTest {

    @Test
    public void testAvroToJson() throws IOException {
        Schema avroArraySchema = SchemaBuilder.array().items(SchemaBuilder.builder().stringType());
        Schema schema = SchemaBuilder.record("record").fields()
                .name("n").type().longType().longDefault(10)
                .name("l").type().longType().longDefault(10)
                .name("i").type().intType().intDefault(10)
                .name("b").type().booleanType().booleanDefault(true)
                .name("bb").type().bytesType().bytesDefault("10")
                .name("d").type().doubleType().doubleDefault(10.0)
                .name("f").type().floatType().floatDefault(10.0f)
                .name("s").type().stringType().stringDefault("titi")
                .name("fi").type().fixed("fi").size(3).fixedDefault(new byte[]{1, 2, 3})
                .name("en").type().enumeration("en").symbols("a", "b", "c").enumDefault("b")
                .name("array").type().optional().array().items(SchemaBuilder.builder().stringType())
                .name("arrayavro").type().optional().array().items(SchemaBuilder.builder().stringType())
                .name("map").type().optional().map().values(SchemaBuilder.builder().intType())
                .name("maputf8").type().optional().map().values(SchemaBuilder.builder().intType())
                .endRecord();
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("n", null);
        genericRecord.put("l", 1L);
        genericRecord.put("i", 1);
        genericRecord.put("b", true);
        genericRecord.put("bb", ByteBuffer.wrap("10".getBytes(StandardCharsets.UTF_8)));
        genericRecord.put("d", 10.0);
        genericRecord.put("f", 10.0f);
        genericRecord.put("s", "toto");
        genericRecord.put("fi", GenericData.get().createFixed(null, new byte[]{'a', 'b', 'c'},
                schema.getField("fi").schema()));
        genericRecord.put("en", GenericData.get().createEnum("b", schema.getField("en").schema()));
        genericRecord.put("array", new String[] {"toto"});
        genericRecord.put("arrayavro", new GenericData.Array<>(avroArraySchema, Arrays.asList("toto")));
        genericRecord.put("map", ImmutableMap.of("a", 10));
        genericRecord.put("maputf8", ImmutableMap.of(new org.apache.avro.util.Utf8("a"), 10));
        JsonNode jsonNode = JsonConverter.toJson(genericRecord, false);
        assertEquals(jsonNode.get("n"), NullNode.getInstance());
        assertEquals(jsonNode.get("l").asLong(), 1L);
        assertEquals(jsonNode.get("i").asInt(), 1);
        assertEquals(jsonNode.get("b").asBoolean(), true);
        assertEquals(jsonNode.get("bb").binaryValue(), "10".getBytes(StandardCharsets.UTF_8));
        assertEquals(jsonNode.get("fi").binaryValue(), "abc".getBytes(StandardCharsets.UTF_8));
        assertEquals(jsonNode.get("en").textValue(), "b");
        assertEquals(jsonNode.get("d").asDouble(), 10.0);
        assertEquals(jsonNode.get("f").numberValue(), 10.0f);
        assertEquals(jsonNode.get("s").asText(), "toto");
        assertTrue(jsonNode.get("array").isArray());
        assertEquals(jsonNode.get("array").iterator().next().asText(), "toto");
        assertTrue(jsonNode.get("arrayavro").isArray());
        assertEquals(jsonNode.get("arrayavro").iterator().next().asText(), "toto");
        assertTrue(jsonNode.get("map").isObject());
        assertEquals(jsonNode.get("map").elements().next().asText(), "10");
        assertEquals(jsonNode.get("map").get("a").numberValue(), 10);
        assertTrue(jsonNode.get("maputf8").isObject());
        assertEquals(jsonNode.get("maputf8").elements().next().asText(), "10");
        assertEquals(jsonNode.get("maputf8").get("a").numberValue(), 10);
    }

    @Test
    public void testLogicalTypesToJson() throws IOException {
        Schema dateType = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        Schema timestampMillisType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        Schema timestampMicrosType = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema timeMillisType = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
        Schema timeMicrosType = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema uuidType = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
        Schema schema = SchemaBuilder.record("record")
                .fields()
                .name("mydate").type(dateType).noDefault()
                .name("tsmillis").type(timestampMillisType).noDefault()
                .name("tsmicros").type(timestampMicrosType).noDefault()
                .name("timemillis").type(timeMillisType).noDefault()
                .name("timemicros").type(timeMicrosType).noDefault()
                .name("myuuid").type(uuidType).noDefault()
                .endRecord();

        final long millisPerDay = 24 * 60 * 60 * 1000;
        BigDecimal myDecimal = new BigDecimal("100.003");
        UUID myUuid = UUID.randomUUID();
        Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("Europe/Copenhagen"));
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("mydate", (int) calendar.toInstant().getEpochSecond());
        genericRecord.put("tsmillis", calendar.getTimeInMillis());
        genericRecord.put("tsmicros", calendar.getTimeInMillis() * 1000);
        genericRecord.put("timemillis", (int) (calendar.getTimeInMillis() % millisPerDay));
        genericRecord.put("timemicros", (calendar.getTimeInMillis() % millisPerDay) * 1000);
        genericRecord.put("myuuid", myUuid.toString());

        GenericRecord genericRecord2 = deserialize(serialize(genericRecord, schema), schema);
        JsonNode jsonNode = JsonConverter.toJson(genericRecord2, false);
        assertEquals(jsonNode.get("mydate").asInt(), calendar.toInstant().getEpochSecond());
        assertEquals(jsonNode.get("tsmillis").asInt(), (int) calendar.getTimeInMillis());
        assertEquals(jsonNode.get("tsmicros").asLong(), calendar.getTimeInMillis() * 1000);
        assertEquals(jsonNode.get("timemillis").asInt(), (int) (calendar.getTimeInMillis() % millisPerDay));
        assertEquals(jsonNode.get("timemicros").asLong(), (calendar.getTimeInMillis() % millisPerDay) * 1000);
        assertEquals(UUID.fromString(jsonNode.get("myuuid").asText()), myUuid);
    }

    public static byte[] serialize(GenericRecord record, Schema schema) throws IOException {
        SpecificDatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
        datumWriter.write(record, binaryEncoder);
        binaryEncoder.flush();
        return byteArrayOutputStream.toByteArray();
    }

    public static GenericRecord  deserialize(byte[] recordBytes, Schema schema) throws IOException {
        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
        ByteArrayInputStream stream = new ByteArrayInputStream(recordBytes);
        BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(stream, null);
        return datumReader.read(null, binaryDecoder);
    }
}