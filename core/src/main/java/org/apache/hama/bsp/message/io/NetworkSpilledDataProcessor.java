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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPMessageBundle;
import org.apache.hama.bsp.message.MessageManager;

public class NetworkSpilledDataProcessor<M extends Writable> extends
    WriteSpilledDataProcessor {
  public static Log LOG = LogFactory.getLog(NetworkSpilledDataProcessor.class);

  private InetSocketAddress targetAddress;
  private MessageManager<M> messenger;
  private Configuration conf;
  // M writableObject;
  IterableSpilledByteBuffer<M> iterator;

  public NetworkSpilledDataProcessor(String fileName)
      throws FileNotFoundException {
    super(fileName);
  }

  public NetworkSpilledDataProcessor(MessageManager<M> messenger,
      InetSocketAddress targetAddress) throws FileNotFoundException {
    super(null);
    this.messenger = messenger;
    this.targetAddress = targetAddress;
  }

  @Override
  public boolean init(Configuration conf) {
    if (!super.init(conf)) {
      return false;
    }
    this.conf = conf;
    String className = conf.get(Constants.MESSAGE_CLASS);

    if (className != null) {
      iterator = new IterableSpilledByteBuffer<M>(className);
    }
    return true;
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public boolean handleSpilledBuffer(SpilledByteBuffer buffer) {
    try {
      iterator.set(buffer);
    } catch (IOException e1) {
      LOG.error("Error setting buffer for combining data", e1);
      return false;
    }

    long i = 0;
    long bundleThreshold = conf.getLong("hama.messenger.bundle.threshold",
        1048576);
    BSPMessageBundle<M> bundle = new BSPMessageBundle<M>();
    for (M item : iterator) {
      i++;
      if (i <= bundleThreshold) {
        bundle.addMessage(item);
      } else {
        try {
          messenger.transfer(targetAddress, bundle);
        } catch (Exception e) {
          LOG.error("Error while sending messages", e);
        }
        bundle = new BSPMessageBundle<M>();
        bundle.addMessage(item);
        i = 1;
      }
    }

    if (i > 0) {
      try {
        messenger.transfer(targetAddress, bundle);
      } catch (Exception e) {
        LOG.error("Error while sending messages", e);
      }
    }

    try {
      iterator.prepareForNext();
    } catch (IOException e1) {
      LOG.error("Error preparing for next buffer.", e1);
      return false;
    }

    return true;
  }
}
