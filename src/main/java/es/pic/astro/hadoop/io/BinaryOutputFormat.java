/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.pic.astro.hadoop.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

public class BinaryOutputFormat<K extends WritableComparable, V extends Writable> extends FileOutputFormat<K, V>
    implements HiveOutputFormat<K, V> {

  private static void writeBinary(OutputStream outStream, Writable value) throws IOException {
    if (!(value instanceof BytesWritable)) {
      throw new IOException("BinaryOutputFormat only supports BytesWritable values, got: "
          + (value == null ? "null" : value.getClass().getName()));
    }

    BytesWritable bw = (BytesWritable) value;
    outStream.write(bw.getBytes(), 0, bw.getLength());
  }

  private static RecordWriter createHiveWriter(final OutputStream outStream) {
    return new RecordWriter() {
      @Override
      public void write(Writable value) throws IOException {
        writeBinary(outStream, value);
      }

      @Override
      public void close(boolean abort) throws IOException {
        outStream.close();
      }
    };
  }

  private static class BinaryRecordWriter<K extends WritableComparable, V extends Writable>
      implements org.apache.hadoop.mapred.RecordWriter<K, V> {

    private final OutputStream outStream;

    BinaryRecordWriter(OutputStream outStream) {
      this.outStream = outStream;
    }

    @Override
    public synchronized void write(K key, V value) throws IOException {
      writeBinary(outStream, value);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      outStream.close();
    }
  }

  /**
   * Creates the final output file and writes rows as raw binary records.
   *
   * Compression is explicitly not supported by this output format.
   */
  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path outPath, Class<? extends Writable> valueClass,
      boolean isCompressed, Properties tableProperties, Progressable progress) throws IOException {

    if (isCompressed) {
      throw new IOException("BinaryOutputFormat does not support compressed output");
    }

    FileSystem fs = outPath.getFileSystem(jc);
    OutputStream outStream = fs.create(outPath, progress);
    return createHiveWriter(outStream);
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name,
      Progressable progress) throws IOException {

    if (getCompressOutput(job)) {
      throw new IOException("BinaryOutputFormat does not support compressed output");
    }

    Path outputPath = FileOutputFormat.getTaskOutputPath(job, name);
    FileSystem fs = outputPath.getFileSystem(job);
    OutputStream outStream = fs.create(outputPath, progress);
    return new BinaryRecordWriter<K, V>(outStream);
  }
}
