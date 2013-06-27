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

package org.apache.hama.bsp.message.queue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.io.NetworkSpilledDataProcessor;
import org.apache.hama.bsp.message.io.SpilledDataProcessor;
import org.apache.hama.bsp.message.io.SpillingDataOutputBuffer;

public class SpillingNetworkQueue<M extends Writable> extends SpillingQueue<M>
    implements DirectQueue<M> {
  private static final Log LOG = LogFactory.getLog(SpillingNetworkQueue.class);

  private class NullSpillIterator implements Iterator<M> {

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public M next() {
      return null;
    }

    @Override
    public void remove() {
      // do nothing
    }

  }

  @Override
  public void setTargetAddress(MessageManager<M> messenger,
      InetSocketAddress target) {
    SpilledDataProcessor processor;
    try {
      processor = new NetworkSpilledDataProcessor<M>(messenger, target);
      processor.init(conf);
    } catch (FileNotFoundException e) {
      LOG.error("Error initializing spilled data stream.", e);
      throw new RuntimeException(e);
    }
    spillOutputBuffer = new SpillingDataOutputBuffer(bufferCount, bufferSize,
        threshold, direct, processor);
  }

  @Override
  public M poll(M msg) {
    return null;
  }

  @Override
  public M poll() {
    return null;
  }

  @Override
  public void prepareRead() {
    try {
      spillOutputBuffer.close();
    } catch (IOException e) {
      LOG.error("Error closing spilled buffer", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public Iterator<M> iterator() {
    return new NullSpillIterator();
  }
}
