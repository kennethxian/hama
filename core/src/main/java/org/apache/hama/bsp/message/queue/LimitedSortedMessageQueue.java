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

import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.message.bundle.BSPMessageBundle;
import org.apache.hama.bsp.message.bundle.POJOMessageBundle;

/**
 * Heap (Java's priority queue) based message queue implementation that supports
 * sorted receive and send.
 */
public final class LimitedSortedMessageQueue<M extends WritableComparable<M>>
    implements MessageQueue<M>, BSPMessageInterface<M> {
  protected static final Log LOG = LogFactory.getLog(LimitedSortedMessageQueue.class);
  private final HamaPriorityQueue<M> queue = new HamaPriorityQueue<M>();
  private Configuration conf;
  private MessageFilter<M> messageFilter;
  
  @Override
  public Iterator<M> iterator() {
    return queue.iterator();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void addAll(Iterable<M> col) {
    for (M m : col)
      queue.add(m);
  }

  @Override
  public void addAll(MessageQueue<M> otherqueue) {
    M poll = null;
    while ((poll = otherqueue.poll()) != null) {
      queue.add(poll);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void add(M item) {
    String filterName = conf.get(Constants.MESSAGE_FILTER_CLASS);
    if (filterName != null) {
      if (messageFilter == null) {
      try {
        messageFilter = (MessageFilter<M>) ReflectionUtils.newInstance(
          conf.getClassByName(filterName), conf);
      } catch (ClassNotFoundException exp) {
        RuntimeException wrap = new RuntimeException("message filter class " + filterName
          + " not found");
         wrap.initCause(exp);
        throw wrap;
      }
    }
      M filtedOutItem = messageFilter.filterOut(item);
      if ((filtedOutItem != null) && (filtedOutItem != item)) {
        queue.removeEq(filtedOutItem);
      } else if ((filtedOutItem != null) && (filtedOutItem == item)) {
        return;
      }
    }
    queue.add(item);
  }

  @Override
  public void clear() {
    queue.clear();
    if (messageFilter != null) {
      messageFilter.clearMessage();
    }
  }

  @Override
  public M poll() {
    //return queue.poll();
    M ret;
    ret = queue.poll();
    if ((ret != null) && (messageFilter != null)) {
      messageFilter.removeMessage(ret);
    }
    return ret;
  }

  @Override
  public int size() {
    return queue.size();
  }

  // empty, not needed to implement

  @Override
  public void init(Configuration conf, TaskAttemptID id) {

  }

  @Override
  public void close() {
    if (messageFilter != null) {
      messageFilter.clearMessage();
    }
  }

  @Override
  public void prepareRead() {

  }

  @Override
  public void prepareWrite() {

  }

  @Override
  public boolean isMessageSerialized() {
    return false;
  }

  @Override
  public void add(BSPMessageBundle<M> bundle) {
    addAll((POJOMessageBundle<M>)bundle);
  }
}
