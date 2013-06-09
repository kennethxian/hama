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

import org.apache.hadoop.io.WritableComparable;

public class SogouBackLinkItem<M extends WritableComparable<M>> implements Comparable<SogouBackLinkItem<M>> {

  public String siteId;
  public double weight;
  public double normalizedWeight;
  public double spreadSiteRank;
  public M originalMsg;

  public SogouBackLinkItem(M initOrgMsg) {
    siteId = null;
    normalizedWeight = 0;
    spreadSiteRank = 0;
    originalMsg = initOrgMsg;
  }

  public double getContribution() {
    return normalizedWeight * spreadSiteRank;
  }

  //@Override
  public int compareTo(SogouBackLinkItem<M> o) {
    return Double.compare(this.getContribution(), o.getContribution());
  }

  public String formatToString(){
    StringBuffer buf = new StringBuffer();
    buf.append(this.siteId);
    buf.append('|');
    buf.append(String.format("%.2f", this.weight));
    buf.append('|');
    buf.append(String.format("%.6f", this.normalizedWeight));
    buf.append('|');
    buf.append(String.format("%.6f", this.spreadSiteRank));
    buf.append('|');
    return buf.toString();
  }
  public void formatFromString(String data) {
    int beginIndex = 0; int endIndex = data.indexOf("|", beginIndex);
    siteId = data.substring(beginIndex, endIndex);
    beginIndex = endIndex+1; endIndex = data.indexOf("|", beginIndex);
    weight = Double.parseDouble(data.substring(beginIndex, endIndex));
    beginIndex = endIndex+1; endIndex = data.indexOf("|", beginIndex);
    normalizedWeight = Double.parseDouble(data.substring(beginIndex, endIndex));
    beginIndex = endIndex+1; endIndex = data.indexOf("|", beginIndex);
    spreadSiteRank = Double.parseDouble(data.substring(beginIndex, endIndex));
  }
}

