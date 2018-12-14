package com.alibabacloud.polar_race.engine.common.neoremind;

import com.alibabacloud.polar_race.engine.common.neoremind.impl.ShardDBImpl;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Pair;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_KEY;
import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@Ignore
public class ConcurrentShardDBImplTest extends BaseDBImplTest {

  @Test
  public void testConcurrentWriteThenRead_WaitWriteToFinish_BigData() throws InterruptedException, ExecutionException, IOException, TimeoutException {
    concurrentWriteThenRead(20, 40000);
  }

  public Map<ByteBuffer, byte[]> concurrentWrite(int concurrency, int totalWrites) throws InterruptedException, IOException, ExecutionException, TimeoutException {
    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
    CompletionService<Pair<ByteBuffer, byte[]>> completionService = new ExecutorCompletionService<>(pool);
    final CountDownLatch countDownLatch = new CountDownLatch(totalWrites);

    try {
      Map<ByteBuffer, byte[]> expected = new ConcurrentHashMap<>();

      long start = System.currentTimeMillis();
      try (DB db = new ShardDBImpl(BASE_PATH, 1024)) {
        for (int i = 0; i < totalWrites; i++) {
          completionService.submit(() -> {
            try {
              byte[] key = BenchMarkUtil.getRandomKey(SIZE_OF_KEY);
              ByteBuffer value = ByteBuffer.allocate(SIZE_OF_VALUE);
              value.put(key);
              db.write(key, value.array());
              expected.put(ByteBuffer.wrap(key), value.array());
              return Pair.of(ByteBuffer.wrap(key), value.array());
            } catch (Exception e) {
              e.printStackTrace();
              return null;
            } finally {
              countDownLatch.countDown();
            }
          });
        }
        countDownLatch.await();
      }
      System.out.println("write using " + (System.currentTimeMillis() - start) + "ms");
      assertThat(expected.size(), is(totalWrites));
      return expected;
    } finally {
      pool.shutdown();
    }
  }

  public void concurrentWriteThenRead(int concurrency, int totalWrites) throws InterruptedException, IOException, ExecutionException, TimeoutException {
    Map<ByteBuffer, byte[]> expected = concurrentWrite(concurrency, totalWrites);
    System.out.println("start to read");
    read(expected, concurrency, totalWrites);
  }

  private void read(Map<ByteBuffer, byte[]> expected, int concurrency, int totalWrites) throws IOException, InterruptedException, ExecutionException, TimeoutException {
    assertThat(expected.size(), is(totalWrites));
    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
    CompletionService<String> completionService = new ExecutorCompletionService<>(pool);
    final CountDownLatch countDownLatch = new CountDownLatch(totalWrites);
    long start = System.currentTimeMillis();
    try (DB db = new ShardDBImpl(BASE_PATH, 1024)) {
      for (Map.Entry<ByteBuffer, byte[]> entry : expected.entrySet()) {
        completionService.submit(() -> {
          try {
            assertThat(db.read(entry.getKey().array()), is(entry.getValue()));
            return "OK";
          } catch (Exception e) {
            e.printStackTrace();
            return "FAIL";
          } finally {
            // System.out.println(bytesToHex(entry.getKey().array()));
            countDownLatch.countDown();
          }
        });
      }
      countDownLatch.await();
    }

    System.out.println("finish all read tasks");

    for (int i = 0; i < totalWrites; i++) {
      assertThat(completionService.take().get(60, TimeUnit.SECONDS), is("OK"));
    }

    System.out.println("read using " + (System.currentTimeMillis() - start) + "ms");
  }

}
