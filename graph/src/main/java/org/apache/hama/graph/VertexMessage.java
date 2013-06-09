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

import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.Constants;

public class VertexMessage<M extends WritableComparable<M>> {
  protected static final Log LOG = LogFactory.getLog(VertexMessage.class);
  private static int MAX_NUM_BL_SITE_PER_DOMAIN = 1000;
  private static int MAX_NUM_BL_SITE = 1000000;
  HashSet<String> domainSet = new HashSet<String>();
  public PriorityQueue<SogouBackLinkItem<M>> topBlInfos = new PriorityQueue<SogouBackLinkItem<M>>();
  
  public VertexMessage(Configuration config) {
    if (config != null) {
      MAX_NUM_BL_SITE_PER_DOMAIN = config.getInt(Constants.MESSAGE_FILTER_DOMAIN_PER_VERTEX, MAX_NUM_BL_SITE_PER_DOMAIN);
      MAX_NUM_BL_SITE = config.getInt(Constants.MESSAGE_FILTER_MESSAGE_PER_VERTEX, MAX_NUM_BL_SITE);
    }
  }

  public void removeMessage(SogouBackLinkItem<M> item) {
    SogouDocID docId = new SogouDocID();
    docId.setSiteId(item.siteId);
    String domainId = docId.getDomainId();
    domainSet.remove(domainId);
    //boolean ret = topBlInfos.remove(item);
    Iterator<SogouBackLinkItem<M>> listIter = topBlInfos.iterator();
    while (listIter.hasNext()) {
      SogouBackLinkItem<M> listItem = listIter.next();
      if ((listItem.normalizedWeight == item.normalizedWeight) && 
        (listItem.spreadSiteRank == item.spreadSiteRank)) {
        listIter.remove();
        break;
      }
    }
  }
  
  public void clearMessage() {
    domainSet.clear();
    topBlInfos.clear();
  }
  
  public M filterOut(SogouBackLinkItem<M> item) {
    SogouDocID docId = new SogouDocID();
    docId.setSiteId(item.siteId);
    String domainId = docId.getDomainId();
    boolean accepted1 = false, accepted2 = false;
    SogouBackLinkItem<M> removedItem = null;
    if ((domainSet.size() < MAX_NUM_BL_SITE_PER_DOMAIN) && (!domainSet.contains(domainId))) {
      domainSet.add(domainId);
      accepted1 = true;
    }

    if (topBlInfos.size() < MAX_NUM_BL_SITE) {
      topBlInfos.add(item);
      accepted2 = true;
    } else {
      if (item.compareTo(topBlInfos.peek()) > 0) {
        removedItem = topBlInfos.remove();
        topBlInfos.offer(item);
      } else {
        removedItem = item;
      }
    }
    
    if (accepted1 || accepted2) {
      return null;
    } else {
      LOG.info("Filter out message: " + removedItem.originalMsg);
      return removedItem.originalMsg;
    }
  }
}
