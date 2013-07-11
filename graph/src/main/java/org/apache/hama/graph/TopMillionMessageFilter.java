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

package org.apache.hama.graph;

import java.lang.reflect.Field;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.bsp.message.queue.MessageFilter;

public class TopMillionMessageFilter<M extends WritableComparable<M>>
    implements MessageFilter<M>, Configurable {
  protected static final Log LOG = LogFactory
      .getLog(TopMillionMessageFilter.class);
  HashMap<String, VertexMessage<M>> filterMap = new HashMap<String, VertexMessage<M>>();
  Configuration conf;

  protected SogouBackLinkItem<M> parseMessage(M message) {
    GraphJobMessage convertedMessage = (GraphJobMessage) message;
    if (!convertedMessage.isVertexMessage()) {
      return null;
    }
    Writable writableMsg = convertedMessage.getVertexValue();
    SogouBackLinkItem<M> item = null;

    if (writableMsg instanceof Text) {
      Text text = (Text) writableMsg;
      String line = text.toString();
      if (line.startsWith("bl:")) {
        item = new SogouBackLinkItem<M>(message);
        item.formatFromString(line.substring(3));
      }
    } else if (writableMsg.getClass().getName()
        .equals("com.sogou.web.hama.siterank.VertexInfo")) {
      Field flagField = null;
      try {
        flagField = writableMsg.getClass().getField("flag");
      } catch (SecurityException e1) {
        e1.printStackTrace();
      } catch (NoSuchFieldException e1) {
        e1.printStackTrace();
      }
      int flag = -1;
      try {
        flag = flagField.getInt(writableMsg);
      } catch (IllegalArgumentException e1) {
        e1.printStackTrace();
      } catch (IllegalAccessException e1) {
        e1.printStackTrace();
      }
      if (flag != 0) {
        return null;
      }

      Field backlinkField = null;
      try {
        backlinkField = writableMsg.getClass().getField("backlinkItem");
      } catch (SecurityException e1) {
        e1.printStackTrace();
      } catch (NoSuchFieldException e1) {
        e1.printStackTrace();
      }

      if (backlinkField == null) {
        return null;
      }

      item = new SogouBackLinkItem<M>(message);
      try {
        Object backlinkObj = backlinkField.get(writableMsg);
        Field[] subFields = backlinkObj.getClass().getDeclaredFields();
        for (Field subField : subFields) {
          String subFieldName = subField.getName();
          if (subFieldName.equals("siteId")) {
            item.siteId = (String) subField.get(backlinkObj);
          } else if (subFieldName.equals("weight")) {
            item.weight = subField.getDouble(backlinkObj);
          } else if (subFieldName.equals("normalizedWeight")) {
            item.normalizedWeight = subField.getDouble(backlinkObj);
          } else if (subFieldName.equals("spreadSiteRank")) {
            item.spreadSiteRank = subField.getDouble(backlinkObj);
          }
        }
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    return item;
  }

  protected VertexMessage<M> findMessageMap(SogouBackLinkItem<M> item) {
    if (filterMap.containsKey(item.siteId)) {
      VertexMessage<M> vertexMsg = filterMap.get(item.siteId);
      return vertexMsg;
    } else {
      VertexMessage<M> vertexMsg = new VertexMessage<M>(conf);
      filterMap.put(item.siteId, vertexMsg);
      return vertexMsg;
    }
  }

  @Override
  public void clearMessage() {
    for (VertexMessage<M> vertexMsg : filterMap.values()) {
      vertexMsg.clearMessage();
    }
  }

  @Override
  public M filterOut(M message) {
    SogouBackLinkItem<M> item = parseMessage(message);
    if (item != null) {
      VertexMessage<M> vertexMsg = findMessageMap(item);
      return vertexMsg.filterOut(item);
    }
    return null;
  }

  @Override
  public void removeMessage(M message) {
    SogouBackLinkItem<M> item = parseMessage(message);
    if (item != null) {
      VertexMessage<M> vertexMsg = findMessageMap(item);
      vertexMsg.removeMessage(item);
    }
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration config) {
    this.conf = config;
  }
}
