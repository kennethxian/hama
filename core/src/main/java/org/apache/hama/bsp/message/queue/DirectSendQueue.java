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

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.MessageManager;

public class DirectSendQueue<M extends Writable> extends POJOMessageQueue<M> {
  private static final Log LOG = LogFactory.getLog(DirectSendQueue.class);
  private ArrayBlockingQueue<M> msgQueue;
  private MessageManager<M> messenger;
  private InetSocketAddress target;
  private boolean closed;
  private Configuration conf;
  private Future<Boolean> dispatchThreadState;

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
  public void init(Configuration conf, TaskAttemptID arg1) {
    long bundleThreshold = conf.getLong("hama.messenger.bundle.threshold",
        1048576);
    int msgQueueLen = bundleThreshold > Integer.MAX_VALUE ? Integer.MAX_VALUE
        : (int) bundleThreshold;
    msgQueue = new ArrayBlockingQueue<M>(msgQueueLen);

    MessageDispatchThread dispatchThread = new MessageDispatchThread();
    ExecutorService dispatchThreadService = Executors.newFixedThreadPool(1);
    dispatchThreadState = dispatchThreadService.submit(dispatchThread);
    this.conf = conf;
  }

  public void setTargetAddress(MessageManager<M> messenger,
      InetSocketAddress target) {
    this.messenger = messenger;
    this.target = target;
  }

  @Override
  public void add(M msg) {
    try {
      msgQueue.put(msg);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public M poll() {
    return null;
  }

  @Override
  public void prepareRead() {
    closed = true;
    boolean completionState = false;
    try {
      completionState = dispatchThreadState.get();
      if (!completionState) {
        throw new RuntimeException(
            "Dispatch Thread failed to complete sucessfully.");
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
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

  class MessageDispatchThread implements Callable<Boolean> {
    private BSPMessageBundle<M> bundle = new BSPMessageBundle<M>();
    long bundleLen;

    MessageDispatchThread() {
    }

    void flushMessage() {
      if (bundleLen > 0) {
        try {
          messenger.transfer(target, bundle);
        } catch (Exception e) {
          LOG.error("Error while sending messages", e);
        }
      }

    }

    void messageSend(M message) {
      long bundleThreshold = conf.getLong("hama.messenger.bundle.threshold",
          1048576);
      if (bundleLen <= bundleThreshold) {
        bundle.addMessage(message);
        bundleLen++;
      } else {
        try {
          messenger.transfer(target, bundle);
        } catch (Exception e) {
          LOG.error("Error while sending messages", e);
        }
        bundle = new BSPMessageBundle<M>();
        bundle.addMessage(message);
        bundleLen = 1;
      }
    }

    @Override
    public Boolean call() throws Exception {
      while ((!closed) || (msgQueue.size() > 0)) {
        M currentMsg;
        try {
          currentMsg = msgQueue.poll(1000, TimeUnit.MILLISECONDS);
          if (currentMsg != null) {
            messageSend(currentMsg);
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      flushMessage();
      return Boolean.TRUE;
    }

  }

  @Override
  public void close() {

  }

  @Override
  public void prepareWrite() {

  }

  @Override
  public void addAll(Iterable<M> col) {
    for (M item : col) {
      add(item);
    }
  }

  @Override
  public void addAll(MessageQueue<M> otherqueue) {
    M poll = null;
    while ((poll = otherqueue.poll()) != null) {
      add(poll);
    }
  }

  @Override
  public void clear() {
  }

  @Override
  public boolean isMessageSerialized() {
    return false;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}
