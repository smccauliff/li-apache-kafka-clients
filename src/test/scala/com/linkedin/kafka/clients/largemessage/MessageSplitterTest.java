/**
 * Copyright 2016 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.kafka.clients.largemessage;

import com.linkedin.kafka.clients.consumer.ExtensibleConsumerRecord;
import com.linkedin.kafka.clients.consumer.HeaderKeySpace;
import com.linkedin.kafka.clients.producer.ExtensibleProducerRecord;
import com.linkedin.kafka.clients.utils.TestUtils;
import com.linkedin.kafka.clients.utils.UUIDFactory;
import com.linkedin.kafka.clients.utils.UUIDFactoryImpl;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit test for message splitter.
 */
public class MessageSplitterTest {
  @Test
  public void testSplit() {
    TopicPartition tp = new TopicPartition("topic", 0);
    UUID id = UUID.randomUUID();
    UUIDFactory uuidFactory = new UUIDFactoryImpl() {
      @Override
      public UUID create() {
        return id;
      }
    };
    String message = TestUtils.getRandomString(1000);
    Serializer<String> stringSerializer = new StringSerializer();
    Deserializer<String> stringDeserializer = new StringDeserializer();
    MessageSplitter splitter = new MessageSplitterImpl(200, uuidFactory, null);

    byte[] serializedMessage = stringSerializer.serialize("topic", message);
    ExtensibleProducerRecord<byte[], byte[]> producerRecord =
        new ExtensibleProducerRecord<byte[], byte[]>("topic", 0, null, "key".getBytes(), serializedMessage);
    Collection<ExtensibleProducerRecord<byte[], byte[]>> records = splitter.split(producerRecord);
    assertEquals(records.size(), 5, "Should have 5 segments.");
    MessageAssembler assembler = new MessageAssemblerImpl(10000, 10000, true);
    Iterator<ExtensibleProducerRecord<byte[], byte[]>> it = records.iterator();
    int expectedSequenceNumber = 0;
    MessageAssembler.AssembleResult assembledMessage = null;
    int totalHeadersSize = 0;

    for (ExtensibleProducerRecord<byte[], byte[]> splitRecord : records) {
      ExtensibleConsumerRecord<byte[], byte[]> splitConsumerRecord = TestUtils.producerRecordToConsumerRecord(splitRecord,
          expectedSequenceNumber, expectedSequenceNumber, null, 0, 0);
      totalHeadersSize += splitRecord.header(HeaderKeySpace.LARGE_MESSAGE_SEGMENT_HEADER).length;
      assembledMessage = assembler.assemble(tp, expectedSequenceNumber, splitConsumerRecord);
      if (expectedSequenceNumber != 5) {
        assertNull(assembledMessage);
      }

      // Check that each segment looks good
      LargeMessageSegment segment =
          new LargeMessageSegment(splitConsumerRecord.header(HeaderKeySpace.LARGE_MESSAGE_SEGMENT_HEADER), ByteBuffer.wrap(splitConsumerRecord.value()));
      assertEquals(segment.messageId(), id, "messageId should match.");
      assertEquals(segment.numberOfSegments(), 5, "number of segments should be 5");
      assertEquals(segment.originalValueSize(), serializedMessage.length, "original value size should the same");
      assertEquals(segment.sequenceNumber(), expectedSequenceNumber, "SequenceNumber should match");
      assertEquals(segment.originalKeyWasNull(), false);
      expectedSequenceNumber++;
    }

    assertTrue(totalHeadersSize != 0);
    assertEquals(assembledMessage.totalHeadersSize(), totalHeadersSize);
    String deserializedOriginalValue = stringDeserializer.deserialize(null, assembledMessage.messageBytes());
    assertEquals(deserializedOriginalValue, message, "values should match.");
  }

}
