package com.alibabacloud.polar_race.engine.common.neoremind.impl.log.reader;

import com.google.common.base.Throwables;

import com.alibabacloud.polar_race.engine.common.neoremind.VLogReader;
import com.alibabacloud.polar_race.engine.common.neoremind.util.Closeables;

import net.smacke.jaydio.DirectRandomAccessFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static com.alibabacloud.polar_race.engine.common.neoremind.SizeOf.SIZE_OF_VALUE;

/**
 * 基于jaydio封装的direct io的vlog reader
 *
 * @author xu.zx
 */
public class DirectIOVLogReader implements VLogReader {

  private final File file;

  private int nonAccurateCounter = 0;

  private DirectRandomAccessFile[] fin;

  public DirectIOVLogReader(File file) {
    this.file = file;
  }

  @Override
  public void init() throws FileNotFoundException {
    try {
      fin = new DirectRandomAccessFile[8];
      for (int i = 0; i < 8; i++) {
        fin[i] = new DirectRandomAccessFile(file, "r", SIZE_OF_VALUE);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public byte[] readValue(long offset) {
    try {
      byte[] value = new byte[SIZE_OF_VALUE];
      int index = (nonAccurateCounter++) & 7;
      DirectRandomAccessFile f = fin[index];
      synchronized (f) {
//      IOUtils.readBytes(value, fileChannel[nonAccurateCounter++ & 3], offset, SIZE_OF_VALUE);
        //fileChannel[nonAccurateCounter++ & 3].read(ByteBuffer.wrap(value, 0, SIZE_OF_VALUE), offset);
        f.seek(offset);
        f.read(value, 0, SIZE_OF_VALUE);
      }
      return value;
    } catch (IOException e) {
      Throwables.propagate(e);
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    for (DirectRandomAccessFile directRandomAccessFile : fin) {
      Closeables.closeQuietly(directRandomAccessFile);
    }
  }
}
