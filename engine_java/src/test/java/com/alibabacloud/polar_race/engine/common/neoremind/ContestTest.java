package com.alibabacloud.polar_race.engine.common.neoremind;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.neoremind.slice.HeapSlice;
import com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil;
import com.alibabacloud.polar_race.engine.common.neoremind.util.FileUtils;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibabacloud.polar_race.engine.common.neoremind.util.BytewiseUtil.bytesToHex;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * 组委会提供的test。
 *
 * @author xu.zx
 */
@Ignore
public class ContestTest {

  @Test
  public void testWriteRead() throws IOException {
    testWrite();
    testRead();
  }

  @Test
  @Ignore
  public void testWriteRange() throws IOException {
    testWrite();
    testRange();
  }

  public void testWrite() throws IOException {
    String dbPath = "/tmp/xyz";
    FileUtils.clearAllFiles(dbPath);
    int threadCount = 64;
    int writeCount = 10000;
    int valueLen = 4096;
    int keyLen = 8;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadCount));
    List<ListenableFuture<Long>> futures = Lists.newArrayList();
    AbstractEngine engine = new EngineRace();

    ListenableFuture<List<Long>> resultsFuture;
    try {
      engine.open(dbPath);
    } catch (EngineException e) {
      e.printStackTrace();
      System.exit(-1);
    }
    try {
      for (int i = 0; i < threadCount; i++) {
        futures.add(executorService.submit(new EngineWriter(writeCount, engine, valueLen, keyLen, i)));
      }
      resultsFuture = Futures.successfulAsList(futures);
      resultsFuture.get();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    engine.close();
  }

  public void testRead() {
    String dbPath = "/tmp/xyz";
    int threadCount = 64;
    int readCount = 10000;
    int valueLen = 4096;
    int keyLen = 8;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadCount));
    List<ListenableFuture<Long>> futures = Lists.newArrayList();
    AbstractEngine engine = new EngineRace();

    ListenableFuture<List<Long>> resultsFuture;
    try {
      engine.open(dbPath);
    } catch (EngineException e) {
      e.printStackTrace();
      System.exit(-1);
    }
    try {
      for (int i = 0; i < threadCount; i++) {
        futures.add(executorService.submit(new EngineReader(readCount, engine, valueLen, keyLen, i)));
      }
      resultsFuture = Futures.successfulAsList(futures);
      resultsFuture.get();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }

    engine.close();
  }

  public void testRange() {
    String dbPath = "/tmp/xyz";
    int threadCount = 64;
    int rangeCount = 2;
    int valueLen = 4096;
    int keyLen = 8;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadCount));
    List<ListenableFuture<Long>> futures = Lists.newArrayList();
    AbstractEngine engine = new EngineRace();

    ListenableFuture<List<Long>> resultsFuture;
    try {
      engine.open(dbPath);
    } catch (EngineException e) {
      e.printStackTrace();
      System.exit(-1);
    }
    try {
      for (int i = 0; i < threadCount; i++) {
        futures.add(executorService.submit(new EngineRanger(rangeCount, engine, valueLen, keyLen, i)));
      }
      resultsFuture = Futures.successfulAsList(futures);
      resultsFuture.get().stream().forEach(System.out::println);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }

    engine.close();
  }

  class EngineWriter implements Callable<Long> {

    int writeCount;

    AbstractEngine engineRace;

    @Override
    public Long call() throws Exception {
      byte[] key = new byte[keyLen];
      byte[] value = new byte[valueLen];
      System.arraycopy(valueBuffer, 0, value, 0, valueLen);
      for (int i = 0; i < writeCount; i++) {
        System.arraycopy(get6BytesData(i, index), 0, key, 0, 6);
        try {
          this.engineRace.write(key, value);

          System.arraycopy(int2byteArray(i), 0, value, 0, 4);
          this.engineRace.write(key, value);
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        }
      }
      return 1L;
    }

    int valueLen;

    int keyLen;

    int index;

    byte[] valueBuffer;

    public EngineWriter(int writeCount, AbstractEngine engineRace, int valueLen, int keyLen, int index) {
      this.writeCount = writeCount;
      this.engineRace = engineRace;
      this.valueLen = valueLen;
      this.keyLen = keyLen;
      this.index = index;
      valueBuffer = String.format("%-" + valueLen + "s", "6").replace(" ", "6").getBytes(StandardCharsets.UTF_8);
    }
  }

  class EngineReader implements Callable<Long> {

    int readCount;

    AbstractEngine engineRace;

    @Override
    public Long call() throws Exception {
      byte[] key = new byte[keyLen];
      byte[] originalValue = new byte[valueLen];
      byte[] currentValue = null;
      System.arraycopy(valueBuffer, 0, originalValue, 0, valueLen);
      for (int i = 0; i < readCount; i++) {
        System.arraycopy(get6BytesData(i, index), 0, key, 0, 6);
        System.arraycopy(int2byteArray(i), 0, originalValue, 0, 4);

        try {
          currentValue = engineRace.read(key);
          //System.out.println(bytesToHex(key));
          if (currentValue == null || currentValue.length != valueLen ||
              !Arrays.equals(currentValue, originalValue)) {
            System.err.println(i + " key = " + bytesToHex(key));
            System.out.println("originalValue=" + bytesToHex(originalValue));
            System.out.println("currentValue=" + bytesToHex(currentValue));
            throw new RuntimeException("not value match " + bytesToHex(key));
          }
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        }
      }
      return 1L;
    }

    int valueLen;

    int keyLen;

    int index;

    byte[] valueBuffer;

    public EngineReader(int writeCount, AbstractEngine engineRace, int valueLen, int keyLen, int index) {
      this.readCount = writeCount;
      this.engineRace = engineRace;
      this.valueLen = valueLen;
      this.keyLen = keyLen;
      this.index = index;
      valueBuffer = String.format("%-" + valueLen + "s", "6").replace(" ", "6").getBytes(StandardCharsets.UTF_8);
    }
  }

  class EngineRanger implements Callable<Long> {

    int rangeCount;

    AbstractEngine engineRace;

    @Override
    public Long call() throws Exception {
      byte[] key = new byte[keyLen];
      byte[] originalValue = new byte[valueLen];
      byte[] currentValue = null;
      System.arraycopy(valueBuffer, 0, originalValue, 0, valueLen);
      AtomicLong xx = new AtomicLong(0);
      engineRace.read(BytewiseUtil.SMALLEST_KEY);
      for (int i = 0; i < rangeCount; i++) {
        try {
          final AtomicReference<HeapSlice> pre = new AtomicReference<>(null);
          engineRace.range(null, null, new AbstractVisitor() {
            @Override
            public void visit(byte[] myKey, byte[] myValue) {
              System.arraycopy(get6BytesData((int) xx.get(), index), 0, key, 0, 6);
              System.arraycopy(int2byteArray((int) xx.get()), 0, originalValue, 0, 4);
              //System.out.println(xx.get() + "===");
              //System.out.println(bytesToHex(myKey));
              //System.out.println(bytesToHex(key));
              if (pre.get() != null) {
                if (HeapSlice.wrappedBuffer(myKey).compareTo(pre.get()) < 0) {
                  System.err.println("pre = " + bytesToHex(pre.get().getBytes()));
                  System.err.println("myKey = " + bytesToHex(myKey));
                }
                if (HeapSlice.wrappedBuffer(myKey).compareTo(pre.get()) < 0) {
                  throw new RuntimeException("not in order" + new String(key));
                }
              }
              if (myKey == null || myKey.length != keyLen ||
                  myValue == null || myValue.length != valueLen
                  || valueEquals(myValue, get6BytesData((int) xx.get(), index), 6)
                  ) {
                System.err.println("pre = " + bytesToHex(pre.get().getBytes()));
                System.err.println("key = " + bytesToHex(key));
                System.err.println("myKey = " + bytesToHex(myKey));
                throw new RuntimeException("not value match" + new String(key));
              }


              //System.out.println(bytesToHex(myKey));
              pre.set(HeapSlice.copiedBuffer(ByteBuffer.wrap(myKey)));
              xx.incrementAndGet();
            }
          });
        } catch (Exception e) {
          e.printStackTrace();
          throw e;
        }
      }
      assertThat(xx.get(), is(1280000L));
      return xx.get();
    }

    int valueLen;

    int keyLen;

    int index;

    byte[] valueBuffer;

    public EngineRanger(int writeCount, AbstractEngine engineRace, int valueLen, int keyLen, int index) {
      this.rangeCount = writeCount;
      this.engineRace = engineRace;
      this.valueLen = valueLen;
      this.keyLen = keyLen;
      this.index = index;
      valueBuffer = String.format("%-" + valueLen + "s", "6").replace(" ", "6").getBytes(StandardCharsets.UTF_8);
    }
  }

  public static boolean valueEquals(byte[] a, byte[] a2, int limit) {
    if (a == a2)
      return true;
    if (a == null || a2 == null)
      return false;

    int length = a.length;
    if (a2.length != length)
      return false;

    for (int i = 0; i < limit; i++)
      if (a[i] != a2[i])
        return false;

    return true;
  }

  /**
   * 251->3f00000000000000
   * 252->3f40000000000000
   * 253->3f80000000000000
   * 254->3fc0000000000000
   * 255->4000000000000000
   * 256->4040000000000000
   * 257->4080000000000000
   */
  static byte[] get6BytesData(int i, int index) {
    ByteBuffer buffer = ByteBuffer.allocate(6);
    buffer.order(ByteOrder.BIG_ENDIAN);
    int mod = i % 6;
    if (mod == 0) {
      buffer.putShort((short) 0x3f00);
    } else if (mod == 1) {
      buffer.putShort((short) 0x3f40);
    } else if (mod == 2) {
      buffer.putShort((short) 0x3f80);
    } else if (mod == 3) {
      buffer.putShort((short) 0x3fc0);
    } else if (mod == 4) {
      buffer.putShort((short) 0x4000);
    } else if (mod == 5) {
      buffer.putShort((short) 0x4040);
    } else if (mod == 6) {
      buffer.putShort((short) 0x4080);
    }
    //    buffer.putShort((short) (14543 + (short) (i % 348)));
    buffer.putInt(i * 10000 + index);
    return buffer.array();
  }

  static byte[] int2byteArray(int i) {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(i);
    return buffer.array();
  }
}
