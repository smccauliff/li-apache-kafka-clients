/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.utils;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.Map;


/**
 * This works with {@link DefaultHeaderDeserializer}.
 *  <pre>
 *  8 bytes magic number
 *  1 byte version/flags
 *  4 byte length of all headers
 *  Repeated: 1 byte key length, utf-8 encoded key bytes, 4 byte value length, value bytes
 *  </pre>
 */
public class DefaultHeaderSerializer implements HeaderSerializer, DefaultHeaderSerde {

  private final byte[] _headerMagic;

  public DefaultHeaderSerializer() {
    this(DefaultHeaderSerde.defaultHeaderMagicBytes());
  }

  /**
   *
   * @param headerMagic  This gets written before the header bytes.  This may be zero length but may not be null.
   */
  protected DefaultHeaderSerializer(byte[] headerMagic) {
    if (headerMagic == null) {
      throw new IllegalArgumentException("headerMagic must not be null");
    }
    _headerMagic = headerMagic;
  }

  /**
   * @param dest The destination byte buffer where we should write the headers to.  If this method throws an exception
   *             the caller can not assume any particular state of dest.
   * @param headers This writes the version field if headers is null.
   */
  @Override
  public void serializeHeader(ByteBuffer dest, Map<String, byte[]> headers, boolean userValueIsNull) {
    int originalPosition = dest.position();
    dest.put(_headerMagic);
    byte versionAndFlags = (byte) (VERSION_1 | (userValueIsNull ? USER_VALUE_IS_NULL_FLAG : 0));
    dest.put(versionAndFlags);
    dest.putInt(0); //updated later
    if (headers == null) {
      return;
    }

    CharsetEncoder utf8Encoder = StandardCharsets.UTF_8.newEncoder();

    int startPosition = dest.position();
    for (Map.Entry<String, byte[]> header : headers.entrySet()) {
      HeaderKeySpace.validateHeaderKey(header.getKey());
      String key = header.getKey();
      int keySize = DefaultHeaderSerde.utf8StringLength(key);
      if (keySize > HeaderKeySpace.MAX_KEY_LENGTH) {
        throw new IllegalArgumentException("Header key \"" + key + "\" is too long.");
      }
      dest.put((byte) keySize);
      serializeHeaderKey(dest, key, utf8Encoder);
      dest.putInt(header.getValue().length);
      dest.put(header.getValue());
    }

    int headerSize = dest.position() - startPosition;
    dest.putInt(originalPosition + _headerMagic.length + VERSION_AND_FLAGS_SIZE, headerSize);
  }

  /**
   * The serialized size of all the headers.
   * @param headers null OK
   * @return VERSION_AND_FLAGS_SIZE if headers is null else the number of bytes needed to represent the header key and value, but
   * without the magic number.
   */
  @Override
  public int serializedHeaderSize(Map<String, byte[]> headers) {
    if (headers == null) {
      return DefaultHeaderSerializer.VERSION_AND_FLAGS_SIZE;
    }
    // size of all the keys and the value length fields
    int size = headers.size() * 5 + VERSION_AND_FLAGS_SIZE + ALL_HEADER_SIZE_FIELD_SIZE + _headerMagic.length;
    for (Map.Entry<String, byte[]> headerEntry : headers.entrySet()) {
      size += headerEntry.getValue().length;
      size += DefaultHeaderSerde.utf8StringLength(headerEntry.getKey());
    }
    return size;
  }

  private void serializeHeaderKey(ByteBuffer dest, String headerKey, CharsetEncoder utf8Encoder) {
    utf8Encoder.reset();
    CoderResult coderResult = utf8Encoder.encode(CharBuffer.wrap(headerKey), dest, true);
    throwErrorFromCoderResult(coderResult, headerKey);
    coderResult = utf8Encoder.flush(dest);
    throwErrorFromCoderResult(coderResult, headerKey);
  }

  private void throwErrorFromCoderResult(CoderResult coderResult, String headerKey) {
    // We should probably hit the underflow result during normal operation so we want to ignore that "error".
    if (coderResult.isMalformed() || coderResult.isOverflow() || coderResult.isUnmappable()) {
      throw new IllegalStateException("Bad encoding for headerKey \"" + headerKey + "\".");
    }
  }

  /** This does nothing */
  @Override
  public void configure(Map<String, ?> configs) {
  }

}
