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
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A file that serializes WritableComparables to a buffer, once it hits a
 * threshold this buffer will be sorted in memory. After the file is closed, all
 * sorted segments are merged to a single file. Afterwards the file can be read
 * using the provided {@link WritableComparable} in order defined in it.
 * 
 * @author thomasjungblut
 * 
 * @param <M> the message type extending WritableComparable.
 */
@SuppressWarnings("rawtypes")
public final class SortedFile<M extends WritableComparable> implements
    IndexedSortable, Closeable {
  private static final Log LOG = LogFactory.getLog(SortedFile.class);

  private static final float SPILL_TRIGGER_BUFFER_FILL_PERCENTAGE = 0.9f;
  private static final IndexedSorter SORTER = new QuickSort();

  private final String dir;
  private final String destinationFileName;
  private final WritableComparator comp;
  private final DataOutputBuffer buf;
  private final int bufferThresholdSize;

  private int fileCount;
  private List<String> files;
  private List<Integer> offsets;
  private List<Integer> indices;
  private int size;
  private boolean mergeFiles;
  private Class<M> msgClass;
  private boolean intermediateMerge;
  private Configuration conf;
  private LocalFileSystem localFileSystem;

  /**
   * Creates a single sorted file. This means, there is no intermediate
   * fileformat, the sorted file can be read with the provided msgClass's
   * read/write methods. The first 4 bytes are the number of times the class was
   * serialized to the file in total. So the output file is not usable for
   * further merging.
   * 
   * @param dir the directory to use for swapping, will be created if not
   *          exists.
   * @param finalFileName the final file where the data should end up merged.
   * @param bufferSize the buffersize. By default, the spill starts when 90% of
   *          the buffer is reached, so you should overallocated ~10% of the
   *          data.
   * @param msgClass the class that implements the comparable, usually the
   *          message class that will be added into collect.
   * @throws IOException in case the directory couldn't be created if not
   *           exists.
   */
  public SortedFile(String dir, String finalFileName, int bufferSize,
      Class<M> msgClass, Configuration conf) throws IOException {
    this(dir, finalFileName, bufferSize, msgClass, true, false, conf);
  }

  /**
   * Creates a single sorted file.
   * 
   * @param dir the directory to use for swapping, will be created if not
   *          exists.
   * @param finalFileName the final file where the data should end up merged.
   * @param bufferSize the buffersize. By default, the spill starts when 90% of
   *          the buffer is reached, so you should overallocated ~10% of the
   *          data.
   * @param msgClass the class that implements the comparable, usually the
   *          message class that will be added into collect.
   * @param intermediateMerge if true, the outputted single sorted file has a
   *          special format so the {@link Merger} can read it for further
   *          merging.
   * @throws IOException in case the directory couldn't be created if not
   *           exists.
   */
  public SortedFile(String dir, String finalFileName, int bufferSize,
      Class<M> msgClass, boolean intermediateMerge, Configuration conf) throws IOException {
    this(dir, finalFileName, bufferSize, msgClass, true, intermediateMerge, conf);
  }

  /**
   * Creates a sorted file.
   * 
   * @param dir the directory to use for swapping, will be created if not
   *          exists.
   * @param finalFileName the final file where the data should end up merged.
   * @param bufferSize the buffersize. By default, the spill starts when 90% of
   *          the buffer is reached, so you should overallocated ~10% of the
   *          data.
   * @param msgClass the class that implements the comparable, usually the
   *          message class that will be added into collect.
   * @param mergeFiles if true the files will be merged at the end.
   * @param intermediateMerge if true, the outputted single sorted file has a
   *          special format so the {@link Merger} can read it for further
   *          merging.
   * @throws IOException in case the directory couldn't be created if not
   *           exists.
   */
  SortedFile(String dir, String finalFileName, int bufferSize,
      Class<M> msgClass, boolean mergeFiles, boolean intermediateMerge, Configuration conf)
      throws IOException {
    this.dir = dir;
    this.destinationFileName = finalFileName;
    this.msgClass = msgClass;
    this.mergeFiles = mergeFiles;
    this.intermediateMerge = intermediateMerge;
    //Files.createDirectories(Paths.get(dir));
    this.conf = conf;
    init(this.dir);
    
    this.bufferThresholdSize = (int) (bufferSize * SPILL_TRIGGER_BUFFER_FILL_PERCENTAGE);
    // create an instance of the msgClass beforehand, so raw comparators are
    // registered
    @SuppressWarnings("unused")
    M instance = ReflectionUtils.newInstance(msgClass, null);
    this.comp = WritableComparator.get(msgClass);
    this.buf = new DataOutputBuffer(bufferSize);
    this.offsets = new ArrayList<Integer>();
    this.indices = new ArrayList<Integer>();
    this.files = new ArrayList<String>();
  }

  public void init(String dir) throws IOException {
    //this.conf = conf;

    localFileSystem = FileSystem.getLocal(conf);
    localFileSystem.mkdirs(new Path(dir));
    /*staticFile = rootPath + "static.graph";
    local.delete(new Path(staticFile), false);
    staticGraphPartsDos = local.create(new Path(staticFile));
    String softGraphFileName = getSoftGraphFileName(rootPath, currentStep);
    local.delete(new Path(softGraphFileName), false);
    softGraphPartsDos = local.create(new Path(dir));*/
  }
  /**
   * Collects a message. If the buffer threshold is exceeded it will sort the
   * buffer and spill to disk. Note that this is synchronous, so this waits
   * until it is finished.
   * 
   * @param msg the message to add.
   * @throws IOException when an IO error happens.
   */
  public void collect(M msg) throws IOException {
    offsets.add(buf.getLength());
    msg.write(buf);
    indices.add(size);
    size++;
    if (buf.getLength() > bufferThresholdSize) {
      sortAndSpill(buf.getLength());
      offsets.clear();
      indices.clear();
      buf.reset();
      size = 0;
    }
  }

  /**
   * First sort, then spill the buffer to disk.
   * 
   * @param bufferEnd the end of the buffer.
   * @throws IOException when IO error happens.
   */
  private void sortAndSpill(int bufferEnd) throws IOException {
    // add the end of the buffer to the list for convenience
    offsets.add(bufferEnd);
    SORTER.sort(this, 0, size);
    // write to file
    //File file = new File(dir, fileCount + ".bin");
    String fileName = dir + "/" + fileCount + ".bin";
    FSDataOutputStream os = localFileSystem.create(new Path(fileName));
    /*try (DataOutputStream os = new DataOutputStream(new BufferedOutputStream(
        new FileOutputStream(file)))) {*/
      // write the size in front, so we can allocate appropriate sized array
      // later on
    try {
      os.writeInt(size);
      for (int index = 0; index < size; index++) {
        // now we have to translate our sorted indices back to the raw bytes
        int x = indices.get(index);
        int off = offsets.get(x);
        int follow = x + 1;
        int len = offsets.get(follow) - off;
        // write the length in front of the record
        WritableUtils.writeVInt(os, len);
        os.write(buf.getData(), off, len);
      } 
    } finally {
      os.close();
    }
    this.files.add(fileName);
    fileCount++;
  } 

  @Override
  public int compare(int left, int right) {
    // calculate the offsets for the data.
    int leftTranslated = indices.get(left);
    int leftEndTranslated = leftTranslated + 1;
    int rightTranslated = indices.get(right);
    int rightEndTranslated = rightTranslated + 1;

    int leftOffset = offsets.get(leftTranslated);
    int rightOffset = offsets.get(rightTranslated);

    int leftLen = offsets.get(leftEndTranslated) - leftOffset;
    int rightLen = offsets.get(rightEndTranslated) - rightOffset;
    return comp.compare(buf.getData(), leftOffset, leftLen, buf.getData(),
        rightOffset, rightLen);
  }

  @Override
  public void swap(int left, int right) {
    // swaps two indices in their files.
    int tmp = indices.get(left);
    indices.set(left, indices.get(right));
    indices.set(right, tmp);
  }

  @Override
  public void close() throws IOException {
    if (buf.getLength() > 0) {
      sortAndSpill(buf.getLength());
    }
    if (mergeFiles) {
      try {
        LOG.info("Starting"
            + (intermediateMerge ? " intermediate" : "") + " merge of "
            + files.size() + " files.");
        Merger.merge(msgClass, intermediateMerge, destinationFileName, files, conf);
      } catch (IOException e) {
        throw e;
      }
    }
  }
  
  public void clear() throws IOException {
    localFileSystem.delete(new Path(dir), true);
    //localFileSystem.delete(new Path(destinationFileName), true);
  }
}
