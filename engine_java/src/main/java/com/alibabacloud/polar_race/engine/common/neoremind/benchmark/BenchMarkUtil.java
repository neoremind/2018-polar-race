package com.alibabacloud.polar_race.engine.common.neoremind.benchmark;

import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class BenchMarkUtil {

  public static String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  public static int VALUE_BUFFER_SIZE = 1024 * 1024;

  public static int VALUE_COUNT = 256;

  public static byte[] getRandomValue() {
    StringBuilder str = new StringBuilder();
    int strLen = ALPHABET.length();
    Random random = new Random(System.nanoTime());
    for (int i = 0; i < VALUE_BUFFER_SIZE; i++) {
      str.append(ALPHABET.charAt(random.nextInt(strLen)));
    }
    return str.toString().getBytes(StandardCharsets.UTF_8);
  }

  public static byte[] getRandomKey(int len, Random random) {
    byte[] bs = new byte[len];
    random.nextBytes(bs);
    return bs;
  }

  public static long getDBPathSize(String path) {
    return FileUtils.sizeOfDirectory(new File(path)) / (1024 * 1024);
  }

  public static void clearDBPath(String path) {
    try {
      Process process = Runtime.getRuntime().exec("cd " + path + " && rm -rf *");
      process.getOutputStream().close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void dropPageCache() {
    try {
      String[] cmdline = {"sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"};
      Process process = Runtime.getRuntime().exec(cmdline);
      process.getOutputStream().close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void getRa() {
    try {
      Process process = Runtime.getRuntime().exec("/sbin/blockdev --getbsz /dev/nvme0n1 && /sbin/blockdev --getra /dev/nvme0n1");
      //取得命令结果的输出流
      InputStream fis = process.getInputStream();
      //用一个读输出流类去读
      InputStreamReader isr = new InputStreamReader(fis);
      //用缓冲器读行
      BufferedReader br = new BufferedReader(isr);
      String line = null;
      //直到读完为止
      while ((line = br.readLine()) != null) {
        System.out.println(line);
      }
      process.getOutputStream().close();
      //throw new RuntimeException("stop here");
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void resetReadAheadBlockSize() {
    try {
      Process process = Runtime.getRuntime().exec("/sbin/blockdev --setra 1024 /dev/nvme0n1 && /sbin/blockdev --setra 1024 /dev/nvme1n1p1 ");
      process.getOutputStream().close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void showDisk() {
    try {
      Process process = Runtime.getRuntime().exec("df -h");
      //取得命令结果的输出流
      InputStream fis = process.getInputStream();
      //用一个读输出流类去读
      InputStreamReader isr = new InputStreamReader(fis);
      //用缓冲器读行
      BufferedReader br = new BufferedReader(isr);
      String line = null;
      //直到读完为止
      while ((line = br.readLine()) != null) {
        System.out.println(line);
      }
      process.getOutputStream().close();
      throw new RuntimeException("stop here");
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    BenchMarkUtil.dropPageCache();
  }
}
