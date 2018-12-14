package com.alibabacloud.polar_race.engine.common.neoremind;

import com.google.common.collect.Lists;

import com.alibabacloud.polar_race.engine.common.neoremind.BenchMarkUtil;
import com.alibabacloud.polar_race.engine.common.neoremind.slice.HeapSlice;
import com.alibabacloud.polar_race.engine.common.neoremind.util.FileUtils;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Pair;

import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_WAL_RECORD;

public abstract class BaseDBImplTest {

  public static final String BASE_PATH = "/tmp/xyz";

  protected byte[] valueBuffer;

  protected int valueIndex = 0;

  protected int writeBufferSizeForNRecords(int n) {
    return n * SIZE_OF_WAL_RECORD;
  }

  /**
   * shuffled
   */
  protected List<Pair<byte[], byte[]>> shuffled512keyAndValueList = Lists.newArrayListWithCapacity(256);

  protected List<Pair<byte[], byte[]>> shuffledManykeyAndValueList = Lists.newArrayListWithCapacity(256);

  @Before
  public void prepare() throws IOException {
    File baseDir = new File(BASE_PATH);
    if (!baseDir.exists()) {
      baseDir.mkdir();
    }

    FileUtils.clearAllFiles(BASE_PATH);

    valueBuffer = BenchMarkUtil.getRandomValue();
    valueIndex = 0;

    initKVList(shuffled512keyAndValueList, 512);
    initKVList(shuffledManykeyAndValueList, 1 << 14);

    for (Pair<byte[], byte[]> pair : shuffledManykeyAndValueList) {
      //System.out.println(bytesToHex(pair.getFirst()));
    }

    Collections.shuffle(shuffled512keyAndValueList);
    Collections.shuffle(shuffledManykeyAndValueList);

    // System.out.println("shuffled512keyAndValueList size is " + shuffled512keyAndValueList.size());
    // System.out.println("shuffledManykeyAndValueList size is " + shuffledManykeyAndValueList.size());
  }

  protected void initKVList(Collection<Pair<byte[], byte[]>> list, int size) {
    for (int i = 0; i < size; i++) {
      HeapSlice key = HeapSlice.allocate(SIZE_OF_KEY);
      key.setLong(0, i);
      HeapSlice value = HeapSlice.allocate(SIZE_OF_VALUE);
      value.setByte(i / 256, i % 256);
      list.add(Pair.of(key.getRawArray(), value.getRawArray()));
    }
  }

  protected byte[] resetValueIndexIfOutOfBounds() {
    if (valueIndex + SIZE_OF_VALUE > valueBuffer.length) {
      valueIndex = 0;
    }
    byte[] bytes = new byte[SIZE_OF_VALUE];
    System.arraycopy(valueBuffer, valueIndex++, bytes, 0, SIZE_OF_VALUE);
    return bytes;
  }

}
