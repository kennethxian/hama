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

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SogouDocID {
  private byte[] id;
  private String strId;
  
  final public static int STRING_DOCID_LENGTH = 66;
  final public static int BYTE_DOCID_LENGTH = 32;
  final public static int STRING_SITEID_LENGTH = 33;
  final public static int BYTE_SITEID_LENGTH = 16;
  final public static int STRING_DOMAINID_LENGTH = 16;
  final public static int BYTE_DOMAINID_LEGNTH = 8;
  final public static int STRING_URLID_LENGTH = 32;
  final public static int BYTE_URLID_LENGTH = 16;

  /**
   */
  public SogouDocID() {
    id = new byte[BYTE_DOCID_LENGTH];
    for( int i = 0; i < BYTE_DOCID_LENGTH; i ++ ) {
      id[i] = 0;
    }
  }
  /**
   */
  public SogouDocID(String docId, int strType) {
    this();
    this.setDocId(docId);
  }
  /**
   */
  public SogouDocID(String url) {
    this();
    this.setUrl(url);
  }
  
  /**
   */
  public void setUrl(String url) {
    if( url == null ) {
      return;
    }
    SogouUrlInfo info = new SogouUrlInfo(url);
    try{
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] b = new byte[16];
      
      info.getDomainSign(md, b, 0, b.length);
      maskFill( b, 0, id, 0, 64 );
      info.getHostSign(md, b, 0, b.length);
      maskFill( b, 0, id, 64, 64 );
      info.getUrlSign(md, b, 0, b.length);
      maskFill( b, 0, id, 128, 128 );
    } catch( NoSuchAlgorithmException e ){
    } catch( DigestException e){
    }
    strId = bytesToString(id);
  }
  
  /**
   */
  public String toString() {
    return getDocId();
  }
  public boolean equals(Object obj) {
    SogouDocID docId2 = (SogouDocID)obj;
    return this.strId.equals(docId2.strId);
  }
  public int hashCode() {
    return this.strId.hashCode();
  }
  
  /**
   */
  public void setDocId(String docId) {
    if(docId == null || docId.length() != STRING_DOCID_LENGTH) {
      return;
    }
    strId = docId;
    id = stringToBytes(strId);
  }
  /**
   */
  public void setDocIdBytes(byte[] docId) {
    if(docId == null || docId.length != BYTE_DOCID_LENGTH) {
      return;
    }
    id = docId;
    strId = bytesToString(id);
  }
  /**
   */
  public String getDocId() {
    return strId;
  }
  /**
   */
  public byte[] getDocIdBytes() {
    return id;
  }
  /**
   */
  public byte[] getDocIdReverseBytes() {
    byte[] rid = new byte[BYTE_DOCID_LENGTH];
    for( int i = 0; i < BYTE_DOCID_LENGTH; i ++ ) {
      rid[i] = id[BYTE_DOCID_LENGTH-i-1];
    }
    return rid;
  }
  
  
  /**
   */
  public String getHostId() {
    return strId.substring(STRING_DOMAINID_LENGTH+1, STRING_SITEID_LENGTH);
  }
  /**
   */
  public void setSiteId(String siteId) {
    if(siteId == null || siteId.length() != STRING_SITEID_LENGTH) {
      return;
    }
    strId = siteId+"-00000000000000000000000000000000";
    id = stringToBytes(strId);
  }
  /**
   */
  public String getSiteId() {
    return strId.substring(0, STRING_SITEID_LENGTH);
  }
  /**
   */
  public byte[] getSiteIdBytes() {
    byte[] siteId = new byte[BYTE_SITEID_LENGTH];
    System.arraycopy(id, 0, siteId, 0, BYTE_SITEID_LENGTH);
    return siteId;
  }
  
  
  /**
   */
  public String getDomainId() {
    return strId.substring(0, STRING_DOMAINID_LENGTH);
  }
  /**
   */
  public byte[] getDomainIdBytes() {
    byte[] domainId = new byte[BYTE_DOMAINID_LEGNTH];
    System.arraycopy(id, 0, domainId, 0, BYTE_DOMAINID_LEGNTH);
    return domainId;
  }
  
  public void setUrlId(String urlId) {
    if(urlId == null || urlId.length() != STRING_URLID_LENGTH) {
      return;
    }
    strId = "0000000000000000-0000000000000000-"+urlId;
    id = stringToBytes(strId);
  }
  /**
   */
  public String getUrlId() {
    return strId.substring(STRING_DOCID_LENGTH-STRING_URLID_LENGTH, STRING_DOCID_LENGTH);
  }
  /**
   */
  public void setUrlIdBytes(byte[] urlId) {
    if(urlId == null || urlId.length != BYTE_URLID_LENGTH) {
      return;
    }
    System.arraycopy(urlId, 0, id, BYTE_DOCID_LENGTH-BYTE_URLID_LENGTH, BYTE_URLID_LENGTH);
    strId = bytesToString(id);
  }
  /**
   */
  public byte[] getUrlIdBytes() {
    byte[] urlId = new byte[BYTE_URLID_LENGTH];
    System.arraycopy(id, BYTE_DOCID_LENGTH-BYTE_URLID_LENGTH, urlId, 0, BYTE_URLID_LENGTH);
    return urlId;
  }
  /**
   */
  public void setReverseUrId(String reverseUrlId) {
    if(reverseUrlId == null || reverseUrlId.length() != STRING_URLID_LENGTH) {
      return;
    }
    String urlId = "";
    for( int i = (STRING_URLID_LENGTH/2)-1; i >= 0 ; i -- ){
      urlId += reverseUrlId.substring(i*2,i*2+2);
    }
    strId = "0000000000000000-0000000000000000-"+urlId;
    id = stringToBytes(strId);
  }
  /**
   */
  public String getReverseUrlId() {
    String urlId = getUrlId();
    String reverseUrlId = "";
    for( int i = (STRING_URLID_LENGTH/2)-1; i >= 0 ; i -- ){
      reverseUrlId += urlId.substring(i*2,i*2+2);
    }
    return reverseUrlId;
  }

  /**
   */
  public long getDocIdRank(){
    return Long.parseLong(strId.substring(50,56),16);
  }
  /**
   */
  public long getSiteIdRank(){
    return Long.parseLong(strId.substring(23,29),16);
  }
  
  
  /**
   */  
  public int getCircleNumberByDocId(int totalCircle) {
    byte res[] = new byte[8];
    for( int i = 0; i < 5; i ++ ) {
      res[7-i] = id[i+16];
    }
    for( int i = 5; i < 8; i ++ ) {
      res[7-i] = 0;
    }
    ByteBuffer bb = ByteBuffer.wrap(res);
    long value = bb.getLong();
    return (int)((((value >> 8) & 0xffffffff) * totalCircle) >> 32);
  }
  
  /**
   */
  public int getCircleNumberBySiteId(int totalCircle) {
    byte res[] = new byte[8];
    for( int i = 0; i < 4; i ++ ) {
      res[i+4] = id[i+8];
    }
    for( int i = 0; i < 4; i ++ ) {
      res[i] = 0;
    }
    ByteBuffer bb = ByteBuffer.wrap(res);
    long value = bb.getLong();
    return (int)((((value) & 0xffffffff) * totalCircle) >> 32);
  }


  /**
   */
  public void setScatterDocIdBySiteId(String scatterDocId) {
    setDocId(scatterDocId);
  }
  /**
   */
  public String getScatterDocIdBySiteId(int scatterLevel) {
    return getDocId();
  }
  /**
   */
  public String[] getScatterDocIdListBySiteId(int scatterLevel) {
    if(scatterLevel <= 0) {
      String[] docIds = new String[1];
      docIds[0] = getSiteId()+"-"+String.format("%032x", 0);
      return docIds;
    }
    if(scatterLevel > 5) {
      scatterLevel = 5;
    }
    int totalSize = 1;
    for( int i = 0; i < scatterLevel; i ++ ) {
      totalSize *= 16;
    }
    String[] scatterDocIdList = new String[totalSize];
    int leftHalfByte = STRING_URLID_LENGTH-scatterLevel;
    String formatter = getSiteId()+"-%0"+scatterLevel+"x%0"+leftHalfByte+"x";
    for( int i = 0; i < totalSize; i ++ ) {
      scatterDocIdList[i] = String.format(formatter, i, 0);
    }
    return scatterDocIdList;
  }
  /**
   */
  public int getCircleNumberByScatterDocIdBySiteId(int totalCircle, int scatterLevel) {
    if(scatterLevel <= 0) {
      return getCircleNumberBySiteId(totalCircle);
    }
    if(scatterLevel > 5) {
      scatterLevel = 5;
    }
    SogouDocID newDocId = new SogouDocID();
    newDocId.setSiteId(this.getDomainId()+"-"
        +this.getUrlId().substring(0,scatterLevel)+this.getHostId().substring(scatterLevel));
    return newDocId.getCircleNumberBySiteId(totalCircle);
  }
  
  private void maskFill( byte[]src, int src_start, byte[] dst, int dst_start, int bit_len){
    for( int i = 0; i < bit_len ; i ++ ){
      updateBit( src, src_start + i, dst, dst_start + i);
    }
  }
  private void updateBit( byte[] src, int src_start, byte[] dst, int dst_start){
    int src_i = (src_start & 0x07);
    int dst_i = (dst_start & 0x07);
    
    int src_index = (src_start >> 3);
    int dst_index = (dst_start >> 3);
    
    int bit = ( (int)src[ src_index ] >> src_i ) & 0x01;
    if( bit == 0 ){
      byte mask = nega_mask[ dst_i ];
      dst[ dst_index ] &= mask;
    } else {
      byte mask = posi_mask[ dst_i ];
      dst[ dst_index ] |= mask;
    }
  }
  private String bytesToString(byte[] key){
    if( key == null || key.length != BYTE_DOCID_LENGTH ) {
      return null;
    }
    char[] buf = new char[key.length*2+2];
    for (int i = 0; i < 8; ++i) {  //DomainSign
      int v = (key[i] > -1) ? key[i] : (key[i] + 0x100);
      buf[2*i] = hexchars[v/0x10];
      buf[2*i+1] = hexchars[v%0x10];
    }
    buf[16] = '-';
    for (int i = 8; i < 16; ++i) {  //HostSign
      int v = (key[i] > -1) ? key[i] : (key[i] + 0x100);      
      buf[1+2*i] = hexchars[v/0x10];
      buf[1+2*i+1] = hexchars[v%0x10];
    }
    buf[33] = '-';
    for (int i = 16; i < 32; ++i) {  //UrlSign
      int v = (key[i] > -1) ? key[i] : (key[i] + 0x100);      
      buf[2+2*i] = hexchars[v/0x10];
      buf[2+2*i+1] = hexchars[v%0x10];
    }
    return new String(buf);
  }
  private byte[] stringToBytes(String key) {
    if( key == null || key.length() != STRING_DOCID_LENGTH ) {
      return null;
    }
    ByteBuffer bb = ByteBuffer.allocate(32);
    int i = 0;
    while(i<key.length()-1){
      if( key.charAt(i) == '-' ){
        i++;
      } else {
        try{
          int a = Integer.parseInt(key.substring(i, i+2), 16);
            bb.put((byte)a);
          i+= 2;
        }catch(Exception e){
          break;
        }
      }
    }
    if( i != key.length() || bb.position() == 0){
      return null;
    } else {
      return bb.array();
    }
  }
  
  private byte[] nega_mask = {(byte)0xFE,(byte)0xFD,(byte)0xFB,(byte)0xF7,
      (byte)0xEF,(byte)0xDF,(byte)0xBF,(byte)0x7F,};
  private byte[] posi_mask = {(byte)0x01,(byte)0x02,(byte)0x04,(byte)0x08,
      (byte)0x10,(byte)0x20,(byte)0x40,(byte)0x80,};
  private char[] hexchars = { '0', '1', '2', '3', '4', '5', '6', '7', 
      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };
      
  /**
   */
  public static String url2docId( String url ){
    SogouDocID docId = new SogouDocID(url);
    return docId.getDocId();
  }
}



