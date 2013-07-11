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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author root
 * 
 */
public class SogouVertexInfo<M extends WritableComparable<M>> implements WritableComparable<SogouVertexInfo<M>> {
  public SogouBackLinkItem<M> backlinkItem;
  public String srMsg;
  // public SiteIDLong sil;
  // public long prefix = 0l;
  // public long suffix = 0l;
  // public String siteId;
  public static final int BACKLINKITEM_FLAG = 0x0;
  public static final int SITEINFO_FLAG = 0X1;
  public int flag = 0;

  /**
         * 
         */
  public SogouVertexInfo(M message) {
    // siteId = null;
    // prefix = 0l;
    // suffix = 0l;
    // siteId = "";
    backlinkItem = new SogouBackLinkItem<M>(message);
    srMsg = "";
  }

  public String formatSiteInfoToString() {
    return srMsg;
  }

  public void formatSiteInfoFromString(String info) {
    srMsg = info;
  }

  public void setFlat(int flag) {
    this.flag = flag;
  }

  // @Override
  public int compareTo(SogouVertexInfo<M> o) {
    if (isBackLinkMessage()) {
      return Double.compare(this.backlinkItem.getContribution(),
          o.backlinkItem.getContribution());
    } else {
      // return this.siteId.compareTo(o.siteId);
      return this.srMsg.compareTo(o.srMsg);
    }

  }

  public String formatBackLinkToString() {
    return backlinkItem.formatToString();
  }

  /**
   * @param args
   */
  public static void main(String[] args) {

  }

  public boolean isBackLinkMessage() {
    return this.flag == BACKLINKITEM_FLAG;
  }

  public boolean isSiteInfoMessage() {
    return this.flag == SITEINFO_FLAG;
  }

  public void formatFromString(String info) {
    if (isBackLinkMessage()) {
      this.backlinkItem.formatFromString(info);
    } else if (isSiteInfoMessage()) {
      this.formatSiteInfoFromString(info);
    }
  }

  public void copyObject(SogouBackLinkItem<M> item) {
    this.backlinkItem.siteId = item.siteId;
    this.backlinkItem.weight = item.weight;
    this.backlinkItem.normalizedWeight = item.normalizedWeight;
    this.backlinkItem.spreadSiteRank = item.spreadSiteRank;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.flag = in.readByte();
    if (isBackLinkMessage()) {
      // this.siteId = in.readUTF();
      this.backlinkItem.siteId = in.readUTF();
      this.backlinkItem.weight = in.readDouble();
      this.backlinkItem.normalizedWeight = in.readDouble();
      this.backlinkItem.spreadSiteRank = in.readDouble();
      // if (this.backlinkItem.siteId
      // .equals("9feb06cfd91d4bbd-0007f58e4a92f311")) {
      // System.out.println("erase siteid:"
      // + this.formatBackLinkToString());
      // }
    } else if (isSiteInfoMessage()) {
      this.srMsg = in.readUTF();
    }

  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(this.flag);
    if (isBackLinkMessage()) {
      // out.writeLong(prefix);
      // out.writeLong(suffix);
      out.writeUTF(this.backlinkItem.siteId);
      out.writeDouble(this.backlinkItem.weight);
      out.writeDouble(this.backlinkItem.normalizedWeight);
      out.writeDouble(this.backlinkItem.spreadSiteRank);
    } else if (isSiteInfoMessage()) {
      // out.writeLong(prefix);
      // out.writeLong(suffix);
      out.writeUTF(srMsg);
    }
  }

  public String toString() {
    if (isBackLinkMessage()) {
      return this.formatBackLinkToString();
    } else if (isSiteInfoMessage()) {
      return this.formatSiteInfoToString();
    } else {
      return "VertexMessage [flag]" + this.flag + ", info=" + srMsg + "]";
    }

  }

}
