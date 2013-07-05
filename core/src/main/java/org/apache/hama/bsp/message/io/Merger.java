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

package org.apache.hama.bsp.message.io;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.PriorityQueue;

/**
 * Sorted segment merger on disk. It maintains a heap to minimize the number of
 * comparisions made between the files.
 * 
 * @author thomas.jungblut
 * 
 * @param <M> the message type extending WritableComparable.
 */
@SuppressWarnings("rawtypes")
public final class Merger<M extends WritableComparable> {
  enum SEGMENT_TYPE {
    MEMORY, FILE
  };

  private static final Log LOG = LogFactory.getLog(Merger.class);

  private final String outputFile;
  private final List<String> mergeFiles;
  private final WritableComparator comp;
  private final boolean intermediateMerge;
  private LocalFileSystem localFileSystem;

  private List<Integer> finalIndices;
  private List<Integer> finalOffsets;
  private DataOutputBuffer finalBuf;
  private int finalSize;

  private Merger(Class<M> msgClass, boolean intermediateMerge,
      String outputFile, List<String> list, Configuration conf,
      List<Integer> indices, List<Integer> offsets, DataOutputBuffer buf,
      int size) throws IOException {
    this.intermediateMerge = intermediateMerge;
    this.outputFile = outputFile;
    this.mergeFiles = list;
    this.comp = WritableComparator.get(msgClass);
    this.localFileSystem = FileSystem.getLocal(conf);
    this.finalIndices = indices;
    this.finalOffsets = offsets;
    this.finalBuf = buf;
    this.finalSize = size;
  }

  /**
   * Merges all given files together.
   */
  private void mergeFiles() throws IOException {
    // just move if we have a single file and intermediate merge turned on
    if ((intermediateMerge && mergeFiles.size() == 1)
        && (finalBuf == null || finalBuf.size() == 0)) {
      localFileSystem.rename(new Path(mergeFiles.get(0)), new Path(outputFile));
      return;
    }

    /*
     * TODO what is faster? Merging two largest segments in a single file until
     * only one is left, or building a large file while iterating over all
     * files?
     */

    // we use a priority queue to track sorted segments and minimize the
    // comparisions between the keys.
    int segmentNum = (finalBuf == null || finalBuf.size() == 0) ? mergeFiles
        .size() : mergeFiles.size() + 1;
    SegmentedPriorityQueue segments = new SegmentedPriorityQueue(segmentNum);
    int sumItems = 0;
    for (int i = 0; i < mergeFiles.size(); i++) {
      Segment segment = new Segment(mergeFiles.get(i));
      segments.put(segment);
      sumItems += segment.getItems();
    }

    // and memory segment
    if (finalBuf != null && finalBuf.size() > 0) {
      LOG.info("Add memory segment.");
      Segment memSegment = new Segment(finalBuf.getData(), finalIndices,
          finalOffsets, finalSize);
      segments.put(memSegment);
      sumItems += memSegment.getItems();
    }
    int active = segmentNum;
    FSDataOutputStream dos = localFileSystem.create(new Path(outputFile));
    /*
     * try (DataOutputStream dos = new DataOutputStream(new
     * BufferedOutputStream( new FileOutputStream(outputFile)))) {
     */
    // write the number of items in front of the merged segments
    try {
      dos.writeInt(sumItems);
      while (active > 0) {
        // merge files together
        Segment peek = segments.top();
        if (peek == null) {
          break;
        }
        if (intermediateMerge) {
          // when intermediate merging, we add the length of the following
          // record to the stream
          WritableUtils.writeVInt(dos, peek.getLength());
        }
        dos.write(peek.getBytes(), peek.getOffset(), peek.getLength());
        if (peek.hasNext()) {
          peek.next();
        } else {
          // if we have nothing to read anymore, close it
          peek.close();
          // pop out of the prio queue
          segments.pop();
          active--;
        }
        // always adjust root of the heap
        segments.adjustTop();
      }
    } finally {
      dos.close();
    }
    if (!intermediateMerge) {
      // delete the temporary files if not intermediate
      for (String file : mergeFiles) {
        // Files.delete(file.toPath());
        localFileSystem.delete(new Path(file), true);
      }
    }
  }

  final class SegmentedPriorityQueue extends PriorityQueue<Segment> {

    public SegmentedPriorityQueue(int items) {
      initialize(items);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected boolean lessThan(Object a, Object b) {
      return ((Segment) a).compareTo(((Segment) b)) < 0;
    }
  }

  final public class Segment implements Comparable<Segment>, Closeable {

    private DataOutputBuffer buf = new DataOutputBuffer();
    private DataInputStream in;
    private int items;
    private int len = -1;
    private SEGMENT_TYPE segmentType;

    private List<Integer> indices;
    private List<Integer> offsets;
    private byte[] memBuf;
    private int currentIndex;

    public Segment(String f) throws IOException {
      // in = new DataInputStream(new BufferedInputStream(new
      // FileInputStream(f)));
      segmentType = SEGMENT_TYPE.FILE;
      in = localFileSystem.open(new Path(f));
      // we read how many items are expected
      items = in.readInt();
      // read the first record length
      len = WritableUtils.readVInt(in);
      // read the first record
      buf.write(in, len);

    }

    public Segment(byte[] in, List<Integer> indices, List<Integer> offsets,
        int size) throws IOException {
      segmentType = SEGMENT_TYPE.MEMORY;
      this.memBuf = in;
      // we read how many items are expected
      this.items = size;
      this.indices = indices;
      this.offsets = offsets;
      getCurrentBuffer();
    }

    private void getCurrentBuffer() {
      int x = indices.get(currentIndex);
      int off = offsets.get(x);
      int follow = x + 1;
      len = offsets.get(follow) - off;
      // write the length in front of the record
      try {
        buf.write(memBuf, off, len);
      } catch (IOException e) {
        e.printStackTrace();
      }
      currentIndex++;
    }

    public byte[] getBytes() {
      return buf.getData();
    }

    // offset is constant zero, because we are resetting the buffer every next()
    // call.
    public int getOffset() {
      return 0;
    }

    public int getLength() {
      return len;
    }

    public int getItems() {
      return this.items;
    }

    public boolean hasNext() {
      // here it is > 1, because we don't decrement items in the constructor for
      // outside item values be read correctly and written correctly.
      return items > 1;
    }

    // sets the record one further in the file
    public void next() throws IOException {
      buf.reset();
      if (segmentType == SEGMENT_TYPE.FILE) {
        len = WritableUtils.readVInt(in);
        buf.write(in, len);
      } else {
        getCurrentBuffer();
      }
      items--;
    }

    @Override
    public int compareTo(Segment o) {
      return comp.compare(getBytes(), getOffset(), getLength(), o.getBytes(),
          o.getOffset(), o.getLength());
    }

    @Override
    public void close() throws IOException {
      if (segmentType == SEGMENT_TYPE.FILE) {
        in.close();
      }
    }

  }

  /*
   * Some helper functions for various types of arguments.
   */

  public static <M extends WritableComparable<?>> void mergeIntermediate(
      Class<M> msgClass, String outputFile, Configuration conf, String... files)
      throws IOException {
    mergeIntermediate(msgClass, outputFile, Arrays.asList(files), conf);
  }

  /*
   * public static <M extends WritableComparable<?>> void mergeIntermediate(
   * Class<M> msgClass, File outputFile, File... files) throws IOException {
   * merge(msgClass, true, outputFile, Arrays.asList(files)); }
   */

  /*
   * public static <M extends WritableComparable<?>> void mergeIntermediate(
   * Class<M> msgClass, File outputFile, List<File> list) throws IOException {
   * merge(msgClass, true, outputFile, list); }
   */

  public static <M extends WritableComparable<?>> void mergeIntermediate(
      Class<M> msgClass, String outputFile, List<String> list,
      Configuration conf) throws IOException {
    merge(msgClass, true, outputFile, list, conf);
  }

  public static <M extends WritableComparable<?>> void merge(Class<M> msgClass,
      String outputFile, Configuration conf, String... files)
      throws IOException {
    merge(msgClass, outputFile, Arrays.asList(files), conf);
  }

  public static <M extends WritableComparable<?>> void merge(Class<M> msgClass,
      String outputFile, List<String> list, Configuration conf)
      throws IOException {
    // List<File> files = toFiles(list);
    merge(msgClass, false, outputFile, list, conf);
  }

  /*
   * public static <M extends WritableComparable<?>> void merge(Class<M>
   * msgClass, File outputFile, File... files) throws IOException {
   * merge(msgClass, false, outputFile, Arrays.asList(files)); }
   */

  /*
   * public static <M extends WritableComparable<?>> void merge(Class<M>
   * msgClass, File outputFile, List<File> list) throws IOException {
   * merge(msgClass, false, outputFile, list); }
   */

  public static <M extends WritableComparable<?>> void merge(Class<M> msgClass,
      boolean intermediateMerge, String outputFile, List<String> list,
      Configuration conf) throws IOException {
    new Merger<M>(msgClass, intermediateMerge, outputFile, list, conf, null,
        null, null, 0).mergeFiles();
  }

  public static <M extends WritableComparable<?>> void merge(Class<M> msgClass,
      boolean intermediateMerge, String outputFile, List<String> list,
      Configuration conf, List<Integer> indices, List<Integer> offsets,
      DataOutputBuffer buf, int size) throws IOException {
    new Merger<M>(msgClass, intermediateMerge, outputFile, list, conf, indices,
        offsets, buf, size).mergeFiles();
  }

}
