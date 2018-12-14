package com.alibabacloud.polar_race.engine.common.neoremind;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.DBImpl;
import com.alibabacloud.polar_race.engine.common.neoremind.impl.ShardDBImpl;
import com.alibabacloud.polar_race.engine.common.neoremind.slice.HeapSlice;
import com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Pair;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;
import static com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil.hexStringToByteArray;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertThat;

@Ignore
public class DBImplTest extends BaseDBImplTest {

  @Test
  public void testOpenThenClose_WriteAndRead_Shard() throws IOException {
    writeCloseThenReadShard(10000);
  }

  @Test
  public void testOpenThenClose_WriteAndRead() throws IOException {
    writeCloseThenRead(40);
  }

  private void writeCloseThenReadShard(int count) throws IOException {
    byte[] key;

    Map<ByteBuffer, byte[]> expected = Maps.newHashMap();
    try (DB db = new ShardDBImpl(BASE_PATH, 1024)) {
      for (int i = 0; i < count; i++) {
        key = BenchMarkUtil.getRandomKey(SIZE_OF_KEY);
        byte[] value = resetValueIndexIfOutOfBounds();
        db.write(key, value);
        expected.put(ByteBuffer.wrap(key), value);
      }
    }

    System.out.println("==write done, close, and open again");
    assertThat(expected.size(), is(count));

    try (DB db = new ShardDBImpl(BASE_PATH, 1024)) {
      System.out.println("==start read");
      for (Map.Entry<ByteBuffer, byte[]> entry : expected.entrySet()) {
        byte[] expectedKey = entry.getKey().array();
        byte[] expectedValue = entry.getValue();
        assertThat(db.read(expectedKey), is(expectedValue));
      }
    }
  }

  private void writeCloseThenRead(int count) throws IOException {
    byte[] key;

    Map<ByteBuffer, byte[]> expected = Maps.newHashMap();
    try (DB db = new DBImpl(BASE_PATH)) {
      for (int i = 0; i < count; i++) {
        key = BenchMarkUtil.getRandomKey(SIZE_OF_KEY);
        byte[] value = resetValueIndexIfOutOfBounds();
        db.write(key, value);
        expected.put(ByteBuffer.wrap(key), value);
      }
    }

    System.out.println("==write done, close, and open again");
    assertThat(expected.size(), is(count));

    try (DBImpl db = new DBImpl(BASE_PATH)) {
      db.prepareRead();
      System.out.println("==start read");
      for (Map.Entry<ByteBuffer, byte[]> entry : expected.entrySet()) {
        byte[] expectedKey = entry.getKey().array();
        byte[] expectedValue = entry.getValue();
        assertThat(db.read(expectedKey), is(expectedValue));
      }
    }
  }

  @Test
  public void testSameKeyOverwrite() throws IOException {
    byte[] key;

    Map<ByteBuffer, byte[]> expected = Maps.newHashMap();
    try (DBImpl db = new DBImpl(BASE_PATH)) {
      db.prepareRead();
      key = hexStringToByteArray("5A FF 00 00 00 00 00 00");
      ByteBuffer buffer = ByteBuffer.allocate(SIZE_OF_VALUE);
      buffer.put(1, (byte) 0xFF);
      db.write(key, buffer.array());
      expected.put(ByteBuffer.wrap(key), buffer.array());

      key = hexStringToByteArray("5A EE 00 00 00 00 00 00");
      buffer = ByteBuffer.allocate(SIZE_OF_VALUE);
      buffer.put(1, (byte) 0xEE);
      db.write(key, buffer.array());
      expected.put(ByteBuffer.wrap(key), buffer.array());

      key = hexStringToByteArray("5A DD 00 00 00 00 00 00");
      buffer = ByteBuffer.allocate(SIZE_OF_VALUE);
      buffer.put(1, (byte) 0xDD);
      db.write(key, buffer.array());
      expected.put(ByteBuffer.wrap(key), buffer.array());

      key = hexStringToByteArray("5A FF 00 00 00 00 00 00");
      buffer = ByteBuffer.allocate(SIZE_OF_VALUE);
      buffer.put(1, (byte) 0xFF);
      db.write(key, buffer.array());
      expected.put(ByteBuffer.wrap(key), buffer.array());

      key = hexStringToByteArray("5A EE 00 00 00 00 00 00");
      buffer = ByteBuffer.allocate(SIZE_OF_VALUE);
      buffer.put(1, (byte) 0x22);
      db.write(key, buffer.array());
      expected.put(ByteBuffer.wrap(key), buffer.array());

      key = hexStringToByteArray("5A DD 00 00 00 00 00 00");
      buffer = ByteBuffer.allocate(SIZE_OF_VALUE);
      buffer.put(1, (byte) 0x33);
      db.write(key, buffer.array());
      expected.put(ByteBuffer.wrap(key), buffer.array());

      key = hexStringToByteArray("5A DD 00 00 00 00 00 00");
      buffer = ByteBuffer.allocate(SIZE_OF_VALUE);
      buffer.put(1, (byte) 0x44);
      db.write(key, buffer.array());
      expected.put(ByteBuffer.wrap(key), buffer.array());
    }

    System.out.println("expected.size() = " + expected.size());
    try (DBImpl db = new DBImpl(BASE_PATH)) {
      db.prepareRead();
      System.out.println("==start read");
      for (Map.Entry<ByteBuffer, byte[]> entry : expected.entrySet()) {
        byte[] expectedKey = entry.getKey().array();
        assertThat(db.read(expectedKey), is(expected.get(ByteBuffer.wrap(expectedKey))));
      }
    }
  }

  @Test
  public void testWriteThenCloseThenRange_Shard() throws IOException {
    writeThenCloseThenRange(shuffled512keyAndValueList,
        null, null, shuffled512keyAndValueList.size());
  }

  //TODO 不适用于并发64线程
  @Test
  public void testWriteThenCloseThenRange_Shard_ManyKvs() throws IOException {
    writeThenCloseThenRange(shuffledManykeyAndValueList,
        null, null, shuffledManykeyAndValueList.size());
  }

  public void writeThenCloseThenRange(List<Pair<byte[], byte[]>> shuffledKVList,
                                      byte[] lower, byte[] upper, int expectedRangeSize) throws IOException {
    Map<HeapSlice, byte[]> expected = Maps.newHashMap();
    try (DB db = new ShardDBImpl(BASE_PATH, 1024)) {
      for (Pair<byte[], byte[]> pair : shuffledKVList) {
        byte[] key = pair.getFirst();
        byte[] value = pair.getSecond();
        db.write(key, value);
        HeapSlice slice = HeapSlice.allocate(SIZE_OF_KEY);
        slice.setBytes(0, key, 0, SIZE_OF_KEY);
        expected.put(slice, value);
      }
    }

    System.out.println("expected.size=" + expected.size());
    assertThat(expected.size(), is(shuffledKVList.size()));

    Set<HeapSlice> visitedKeySet = Sets.newHashSet();

    final AtomicInteger counter = new AtomicInteger(0);
    try (DB db = new ShardDBImpl(BASE_PATH, 1024)) {
      db.range(lower, upper,
          new AbstractVisitor() {
            final AtomicReference<HeapSlice> pre = new AtomicReference<>(null);
            @Override
            public void visit(byte[] key, byte[] value) {
              try {
                HeapSlice slice = HeapSlice.allocate(SIZE_OF_KEY);
                slice.setBytes(0, key, 0, SIZE_OF_KEY);
                if (pre.get() != null) {
                  if (HeapSlice.wrappedBuffer(key).compareTo(pre.get()) <= 0) {
                    System.err.println("pre = " + BytewiseUtil.bytesToHex(pre.get().getBytes()));
                    System.err.println("key = " + BytewiseUtil.bytesToHex(key));
                  }
                  assertThat(HeapSlice.wrappedBuffer(key).compareTo(pre.get()), greaterThan(0));
                }
                assertThat(value, is(expected.get(slice)));
                counter.incrementAndGet();
              } catch (Throwable e) {
                e.printStackTrace();
                Throwables.propagate(e);
              }
            }
          });
      System.out.println("counter=" + counter);
      if (counter.get() != expectedRangeSize) {
        System.out.println("diff are: ");
        Sets.difference(expected.keySet(), visitedKeySet).stream().forEach(
            e -> System.out.println(BytewiseUtil.bytesToHex(e.getBytes())
            ));
      }
      assertThat(counter.get(), is(expectedRangeSize));
    }
  }

}
