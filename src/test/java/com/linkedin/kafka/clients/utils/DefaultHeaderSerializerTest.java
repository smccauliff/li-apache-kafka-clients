/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class DefaultHeaderSerializerTest {
  private static final char UNICODE_REPLACEMENT_CHARACTER = 65533;

  @Test
  public void invalidUtf8MagicTest() throws Exception {
    byte[] headerMagicBytes;
    try (ByteArrayOutputStream bout = new ByteArrayOutputStream(8);
      DataOutputStream dout = new DataOutputStream(bout)) {
      dout.writeLong(DefaultHeaderSerializer.DEFAULT_HEADER_MAGIC);
      headerMagicBytes = bout.toByteArray();
    }

    for (int i = 0; i < headerMagicBytes.length; i++) {
      byte[] subMagic = Arrays.copyOfRange(headerMagicBytes, i, headerMagicBytes.length);
      // Data input stream fail
      try {
        try (DataInputStream din = new DataInputStream(new ByteArrayInputStream(subMagic))) {
          din.readUTF();
        }
        assertFalse(true, "Expected exception.  Should not have reached here" + i);
      } catch (EOFException e) {
      }

      StringDeserializer stringDeserializer = new StringDeserializer();
      String deserializedBadString = stringDeserializer.deserialize("jkkdfj", subMagic);
      char[] expectedReplacementCharacters = new char[subMagic.length];
      Arrays.fill(expectedReplacementCharacters, UNICODE_REPLACEMENT_CHARACTER);
      assertEquals(deserializedBadString, new String(expectedReplacementCharacters));
    }
  }

  @Test
  public void headerSerializationTest() {
    DefaultHeaderSerializer headerSerializer = new DefaultHeaderSerializer();
    ByteBuffer bbuf = ByteBuffer.allocate(1024*16);
    Map<String, byte[]> headers = new HashMap<>();
    String headerValue = "header-value";
    String headerKey = "header-key";
    headers.put(headerKey, headerValue.getBytes());
    assertEquals(headerSerializer.serializedHeaderSize(headers),
        8 /*magic*/+ 1 /*ver*/+ 4/*totallen*/ + 1 /*header-key len*/ + headerKey.length() +  4 /* value len */ +
            headerValue.length());
    headerSerializer.serializeHeader(bbuf, headers, false);
    bbuf.flip();
    assertEquals(bbuf.remaining(), headerSerializer.serializedHeaderSize(headers));

    HeaderDeserializer headerDeserializer = new DefaultHeaderDeserializer();
    HeaderDeserializer.DeserializeResult deserializeResult = headerDeserializer.deserializeHeader(bbuf);
    assertFalse(deserializeResult.value().hasRemaining());
    assertEquals(deserializeResult.headers().size(), 1);
    assertEquals(deserializeResult.headers().get(headerKey), headerValue.getBytes());
  }
}
