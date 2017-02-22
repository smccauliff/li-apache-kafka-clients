/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */
package com.linkedin.kafka.clients.consumer;

import com.linkedin.kafka.clients.utils.DefaultHeaderDeserializer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 *  This is a specialized map backed by a list instead of a more asymptotically efficient data structure as our
 *  benchmarks show that for small numbers of items this will be more efficient than using HashMap.
 *
 *  This map does not check for duplicates in the incoming ByteBuffer.
 *
 */
public class LazyHeaderListMap implements Map<String,byte[]> {


  private static final class Entry implements Map.Entry<String, byte[]> {
    private final String key;
    private final byte[] value;

    public Entry(String key, byte[] value) {
      if (key == null) {
        throw new NullPointerException("key must not be null");
      }
      if (value == null) {
        throw new NullPointerException("value must not be null");
      }

      this.key = key;
      this.value = value;
    }

    @Override
    public String getKey() {
      return key;
    }

    @Override
    public byte[] getValue() {
      return value;
    }

    @Override
    public byte[] setValue(byte[] value) {
      throw new UnsupportedOperationException();
    }
  };

  private List<Map.Entry<String, byte[]>> backingList;

  private ByteBuffer headerSource;

  public LazyHeaderListMap() {
    this((ByteBuffer) null);
  }

  public LazyHeaderListMap(Map<String, byte[]> other) {
    if (other instanceof LazyHeaderListMap) {
      LazyHeaderListMap otherLazyHeaderListMap = (LazyHeaderListMap) other;
      otherLazyHeaderListMap.lazyInit();
      this.backingList = new ArrayList<>(otherLazyHeaderListMap.backingList);
    } else {
      this.backingList = new ArrayList<>(other.size());
      this.backingList.addAll(other.entrySet());
    }
  }

  /**
   * When the map is accessed then headerSource is parsed.
   *
   * @param headerSource  this may be null in which case there are not any headers.
   */
  public LazyHeaderListMap(ByteBuffer headerSource) {
    this.headerSource = headerSource;
  }

  private void lazyInit() {
    if (backingList != null) {
      return;
    }
    if (headerSource == null) {
      //Not using empty list so that this map remains mutable
      backingList = new ArrayList<>(0);
      return;
    }

    backingList = new ArrayList<>();
    DefaultHeaderDeserializer.parseHeader(headerSource, this);
    headerSource = null;

    assert checkDuplicates();
  }

  /**
   * This checks for duplicates in the map.  If you are are calling this function they you have givenup on performance
   * just use a HashMap.  Otherwise only call this for testing , debugging or in an assert.
   * @return true if we are duplicate free.
   */
  private boolean checkDuplicates() {
    Set<String> keysSeen = new HashSet<>(backingList.size() * 2);
    for (Map.Entry<String, byte[]> entry  : backingList) {
      if (keysSeen.contains(entry.getKey())) {
        return false;
      }
      keysSeen.add(entry.getKey());
    }
    return true;
  }

  @Override
  public int size() {
    lazyInit();

    return backingList.size();
  }

  @Override
  public boolean isEmpty() {
    lazyInit();
    return backingList.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    lazyInit();
    for (int i = backingList.size() - 1; i >= 0; i--) {
      if (backingList.get(i).getKey().equals(key)) {
        return true;
      }
    }
    return false;
  }

  /**
   *  This is not supported.
   */
  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] get(Object key) {
    lazyInit();

    for (int i = 0;  i < backingList.size(); i++) {
      if (backingList.get(i).getKey().equals(key)) {
        return backingList.get(i).getValue();
      }
    }
    return null;
  }

  /**
   *  This has O(n) run time as we check for duplicates when this is added.
   *
   * @param key non-null
   * @param value non-null
   * @return  The previous value stored with the key or null if there was no such value.
   */
  @Override
  public byte[] put(String key, byte[] value) {
    lazyInit();

    if (key == null) {
      throw new IllegalArgumentException("null keys are not supported.");
    }
    if (value == null) {
      throw new IllegalArgumentException("null values are not supported.");
    }

    for (int i = 0; i < backingList.size(); i++) {
      if (backingList.get(i).getKey().equals(key)) {
        byte[] previousValue = backingList.get(i).getValue();
        backingList.set(i, new Entry(key, value));
        return previousValue;
      }
    }
    backingList.add(new Entry(key, value));
    return null;
  }

  /**
   * This is an O(n) operation.
   *
   * @param key non-null
   * @return null if the key did not exist.
   */
  @Override
  public byte[] remove(Object key) {
    lazyInit();

    if (key == null) {
      throw new IllegalStateException("key must not be null");
    }
    for (int i = 0; i < backingList.size(); i++) {
      if (backingList.get(i).getKey().equals(key)) {
        int lastIndex = backingList.size() - 1;
        byte[] value = backingList.get(i).getValue();
        backingList.set(i , backingList.get(lastIndex));
        backingList.remove(lastIndex);
        return value;
      }
    }
    return null;
  }

  @Override
  public void putAll(Map<? extends String, ? extends byte[]> m) {
    for (Map.Entry<? extends String, ? extends byte[]> otherEntry : m.entrySet()) {
      put(otherEntry.getKey(), otherEntry.getValue());
    }
  }

  @Override
  public void clear() {
    lazyInit();
    backingList = new ArrayList<>(0);
  }

  @Override
  public Set<String> keySet() {
    lazyInit();

    return new Set<String>() {

      @Override
      public int size() {
        return backingList.size();
      }

      @Override
      public boolean isEmpty() {
        return backingList.isEmpty();
      }

      @Override
      public boolean contains(Object o) {
        return containsKey(o);
      }

      @Override
      public Iterator<String> iterator() {

        return new Iterator<String>() {
          final int backingListSizeAtInitializationTime = backingList.size();
          int nextIndex = 0;

          @Override
          public boolean hasNext() {
            if (backingList.size() != backingListSizeAtInitializationTime) {
              throw new ConcurrentModificationException();
            }

            return nextIndex < backingList.size();
          }

          @Override
          public String next() {
            if (backingList.size() != backingListSizeAtInitializationTime) {
              throw new ConcurrentModificationException();
            }
            return backingList.get(nextIndex++).getKey();
          }
        };
      }

      @Override
      public Object[] toArray() {
        return backingList.toArray(new Object[backingList.size()]);
      }

      @Override
      public <T> T[] toArray(T[] a) {
        return backingList.toArray(a);
      }

      @Override
      public boolean add(String String) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean remove(Object o) {
        return LazyHeaderListMap.this.remove(o) != null;
      }

      @Override
      public boolean containsAll(Collection<?> c) {
        for (Object key : c) {
          if (!containsKey(key)) {
            return false;
          }
        }
        return true;
      }

      @Override
      public boolean addAll(Collection<? extends String> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
        boolean setChanged = false;
        for (Object key : c) {
          setChanged = setChanged || remove(key);
        }

        return setChanged;
      }

      @Override
      public void clear() {
        backingList.clear();
      }
    };
  }

  @Override
  public Collection<byte[]> values() {
    lazyInit();
    return new Collection<byte[]>() {

      @Override
      public int size() {
        return backingList.size();
      }

      @Override
      public boolean isEmpty() {
        return backingList.isEmpty();
      }

      @Override
      public boolean contains(Object o) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterator<byte[]> iterator() {
        return new Iterator<byte[]>() {
          final int backingListSizeAtInitializationTime = backingList.size();
          private int index = 0;

          @Override
          public boolean hasNext() {
            if (backingListSizeAtInitializationTime != backingList.size()) {
              throw new ConcurrentModificationException();
            }
            return index < backingList.size();
          }

          @Override
          public byte[] next() {
            if (backingListSizeAtInitializationTime != backingList.size()) {
              throw new ConcurrentModificationException();
            }

            return backingList.get(index++).getValue();
          }
        };
      }

      @Override
      public Object[] toArray() {
        throw new UnsupportedOperationException();
      }

      @Override
      public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean add(byte[] bytes) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean remove(Object o) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean addAll(Collection<? extends byte[]> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void clear() {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public Set<Map.Entry<String, byte[]>> entrySet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }
}
