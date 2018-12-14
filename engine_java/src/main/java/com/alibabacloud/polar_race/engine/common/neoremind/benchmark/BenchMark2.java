package com.alibabacloud.polar_race.engine.common.neoremind.benchmark;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 可以一步一步跑的程序
 * <pre>
 *   java -server -Xms1800m -Xmx1800m -XX:MaxDirectMemorySize=128m -XX:NewSize=1G -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -Xloggc:gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -cp engine_java-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.alibabacloud.polar_race.engine.common.neoremind.benchmark.BenchMark2 64 1000000 8 4096 /logging/xyz 3
 * </pre>
 *
 * @author xu.zx
 */
public class BenchMark2 {

  private static void usage() {
    String msg = "java -Xms3072m -Xmx3072m  -XX:MaxDirectMemorySize=64m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:NewRatio=1 -cp ./benchmark_example_java-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.alibabacloud.polar_race.benchmark.BenchMark threadCount writeCount keyLen_byte valueLen_byte db_path";
    System.out.println(msg);
  }

  public static void main(String[] args) throws Exception {
    Logger logger = LoggerFactory.getLogger(BenchMark2.class);
    BasicConfigurator.configure();
    if (args.length != 6) {
      usage();
      System.exit(-1);
    }
    int threadCount = Integer.valueOf(args[0]);
    int writeCount = Integer.valueOf(args[1]);
    int keyLen = Integer.valueOf(args[2]);
    int valueLen = Integer.valueOf(args[3]);
    String dbPath = args[4];
    int round = Integer.valueOf(args[5]);

    long fillRandomIOPS = 0;
    long readRandomIOPS = 0;
    long readSeqIOPS = 0;
    int operationSum = writeCount * threadCount;
    int readSeqCount = threadCount;

    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadCount));
    ScheduledExecutorService executor = MoreExecutors.getExitingScheduledExecutorService((ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1));
    List<ListenableFuture<Integer>> futures = Lists.newArrayList();
    ListenableFuture<List<Integer>> resultsFuture;

    AbstractEngine engine = new EngineRace();

    if (round == 1) {
      List<EngineWriter> engineWriters = Lists.newArrayList();
      long writeSeed = 0;
      for (int i = 0; i < threadCount; i++) {
        engineWriters.add(new EngineWriter(writeCount, engine, valueLen, keyLen, operationSum, writeSeed++));
      }
      logger.error("Date:			" + new Date(System.currentTimeMillis()));
      logger.error("Keys:			" + keyLen + " bytes each");
      logger.error("Values:		" + valueLen + " bytes each");
      logger.error("Entries:			" + operationSum);
      logger.error("------------------------------------------------------------------------");
      long startTime = System.currentTimeMillis();
      try {
        engine.open(dbPath);
      } catch (EngineException e) {
        e.printStackTrace();
        System.exit(-1);
      }
      long opendbTime = System.currentTimeMillis();
      logger.error("open db success, used: " + (opendbTime - startTime) + "ms");

      //random write
      logger.error("Fill random");
      long start = System.currentTimeMillis();
      logger.error("DB path:[" + dbPath + "]");
      try {
        for (int i = 0; i < threadCount; i++) {
          futures.add(executorService.submit(engineWriters.get(i)));
        }
        resultsFuture = Futures.successfulAsList(futures);
        resultsFuture.get();
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      } finally {
        executorService.shutdown();
        executor.shutdown();
        engine.close();
      }
      long writeTime = System.currentTimeMillis();
      fillRandomIOPS = 1000L * operationSum / (writeTime - start);
      logger.error("fillrandom cost:" + (writeTime - start) + "ms, IOPS:" + fillRandomIOPS + "ops/second");

      //restartEngine(engine, dbPath);
    }


    if (round == 2) {
      logger.error("Date:			" + new Date(System.currentTimeMillis()));
      logger.error("Keys:			" + keyLen + " bytes each");
      logger.error("Values:		" + valueLen + " bytes each");
      logger.error("Entries:			" + operationSum);
      logger.error("------------------------------------------------------------------------");
      long startTime = System.currentTimeMillis();
      try {
        engine.open(dbPath);
      } catch (EngineException e) {
        e.printStackTrace();
        System.exit(-1);
      }
      long opendbTime = System.currentTimeMillis();
      logger.error("open db success, used: " + (opendbTime - startTime) + "ms");

      //random read
      logger.error("Read random");
      long start = System.currentTimeMillis();
      logger.error("DB path:[" + dbPath + "]");
      try {
        futures = Lists.newArrayList();
        long readSeed = 0;
        for (int i = 0; i < threadCount; i++) {
          futures.add(executorService.submit(new EngineReader(writeCount, engine, keyLen, operationSum, readSeed++)));
        }
        resultsFuture = Futures.successfulAsList(futures);
        List<Integer> readSuccess = resultsFuture.get();
        int readSuccessSum = 0;
        for (Integer readSucces : readSuccess) {
          readSuccessSum += readSucces;
        }
        logger.error("read success (" + readSuccessSum + " of " + (threadCount * writeCount) + "found )");
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      } finally {
        executorService.shutdown();
        executor.shutdown();
        engine.close();
      }

      long readTime = System.currentTimeMillis();
      readRandomIOPS = 1000L * operationSum / (readTime - start);
      logger.error("readrandom cost:" + (readTime - start) + "ms, IOPS:" + readRandomIOPS + "ops/second");

      //restartEngine(engine, dbPath);
    }

    if (round == 3) {
      logger.error("Date:			" + new Date(System.currentTimeMillis()));
      logger.error("Keys:			" + keyLen + " bytes each");
      logger.error("Values:		" + valueLen + " bytes each");
      logger.error("Entries:			" + operationSum);
      logger.error("------------------------------------------------------------------------");
      long startTime = System.currentTimeMillis();
      try {
        engine.open(dbPath);
      } catch (EngineException e) {
        e.printStackTrace();
        System.exit(-1);
      }
      long opendbTime = System.currentTimeMillis();
      logger.error("open db success, used: " + (opendbTime - startTime) + "ms");

      //seq read
      logger.error("Read seq");
      long start = System.currentTimeMillis();
      futures = Lists.newArrayList();
      List<DefaultVisitor> visitors = Lists.newArrayListWithCapacity(64);
      try {
        for (int i = 0; i < readSeqCount; i++) {
          DefaultVisitor visitor = new DefaultVisitor();
          visitors.add(visitor);
          futures.add(executorService.submit(new EngineRanger(engine, visitor, 2)));
        }
        resultsFuture = Futures.successfulAsList(futures);
        resultsFuture.get();
        System.out.println("====> visit count = " + visitors.stream().map(DefaultVisitor::getVisitCount).mapToLong(AtomicLong::get).sum());
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      } finally {
        executorService.shutdown();
        executor.shutdown();
        engine.close();
      }
      long foreachTime = System.currentTimeMillis();
      //readSeqIOPS = (1000 * visitor.visitCount.get()) / (foreachTime - clearForeachPageCacheTime);
      readSeqIOPS = 0;
      logger.error("readseq cost:" + (foreachTime - start) + "ms, IOPS:" + readSeqIOPS + "ops/second visitCount:");
    }

  }

  private static void restartEngine(AbstractEngine engine, String path, Logger logger) {
    try {
      logger.error("restart engine...");
      long startTime = System.currentTimeMillis();
      logger.error("close engine...");
      engine.close();
      long closeTime = System.currentTimeMillis();
      logger.error("close engine done, used:" + (closeTime - startTime) + "ms");

      logger.error("open engine...");
      engine.open(path);
      long openTime = System.currentTimeMillis();
      logger.error("open engine done, used:" + (openTime - closeTime) + "ms");
    } catch (EngineException e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

}
