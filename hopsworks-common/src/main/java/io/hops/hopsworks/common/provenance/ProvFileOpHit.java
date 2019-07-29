/*
 * This file is part of Hopsworks
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
 *
 * Hopsworks is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Hopsworks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see <https://www.gnu.org/licenses/>.
 */
package io.hops.hopsworks.common.provenance;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.elasticsearch.search.SearchHit;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@XmlRootElement
public class ProvFileOpHit implements Comparator<ProvFileOpHit>  {
  private static final Logger LOG = Logger.getLogger(ProvFileOpHit.class.getName());
  public static final TimestampComparator timestampComparator = new TimestampComparator();
  
  private String id;
  private float score;
  private Map<String, Object> map;
  
  private long inodeId;
  private String inodeOperation;
  private String appId;
  private int logicalTime;
  private long timestamp;
  private String readableTimestamp;
  private String inodeName;
  private String xattrName;
  
  public ProvFileOpHit() {}

  public ProvFileOpHit(SearchHit hit) {
    this.id = hit.getId();
    this.score = hit.getScore();
    //the source of the retrieved record (i.e. all the indexed information)
    this.map = hit.getSourceAsMap();
  
    //export the name of the retrieved record from the list
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      //set the name explicitly so that it's easily accessible in the frontend
      switch (entry.getKey()) {
        case ProvElastic.Common.INODE_ID_FIELD:
          this.inodeId = ((Number) entry.getValue()).longValue();
          break;
        case ProvElastic.Common.APP_ID_FIELD:
          this.appId = entry.getValue().toString();
          break;
        case ProvElastic.Common.INODE_OPERATION_FIELD:
          this.inodeOperation = entry.getValue().toString();
          break;
        case ProvElastic.Common.LOGICAL_TIME_FIELD:
          this.logicalTime = ((Number) entry.getValue()).intValue();
          break;
        case ProvElastic.Common.TIMESTAMP_FIELD:
          this.timestamp = ((Number) entry.getValue()).longValue();
          break;
        case ProvElastic.Common.READABLE_TIMESTAMP_FIELD:
          this.readableTimestamp = entry.getValue().toString();
          break;
        case ProvElastic.Common.INODE_NAME_FIELD:
          this.inodeName = entry.getValue().toString();
          break;
        case ProvElastic.Common.XATTR_NAME_FIELD:
          this.xattrName = entry.getValue().toString();
          break;
        case ProvElastic.Common.ENTRY_TYPE_FIELD:
          break;
        default:
          LOG.log(Level.WARNING, "unknown key:{0} value:{1}", new Object[]{entry.getKey(), entry.getValue()});
          break;
      }
    }
  }
  
  @Override
  public int compare(ProvFileOpHit o1, ProvFileOpHit o2) {
    return Float.compare(o2.getScore(), o1.getScore());
  }
  
  
  public String getId() {
    return id;
  }
  
  public void setId(String id) {
    this.id = id;
  }
  
  public float getScore() {
    return score;
  }
  
  public void setScore(float score) {
    this.score = score;
  }
  
  public Map<String, String> getMap() {
    //flatten hits (remove nested json objects) to make it more readable
    Map<String, String> refined = new HashMap<>();
    
    if (this.map != null) {
      for (Map.Entry<String, Object> entry : this.map.entrySet()) {
        //convert value to string
        String value = (entry.getValue() == null) ? "null" : entry.getValue().toString();
        refined.put(entry.getKey(), value);
      }
    }
    
    return refined;
  }
  
  public void setMap(Map<String, Object> map) {
    this.map = map;
  }
  
  
  public long getInodeId() {
    return inodeId;
  }
  
  public void setInodeId(long inodeId) {
    this.inodeId = inodeId;
  }
  
  public String getInodeOperation() {
    return inodeOperation;
  }
  
  public void setInodeOperation(String inodeOperation) {
    this.inodeOperation = inodeOperation;
  }
  
  public String getAppId() {
    return appId;
  }
  
  public void setAppId(String appId) {
    this.appId = appId;
  }
  
  public int getLogicalTime() {
    return logicalTime;
  }
  
  public void setLogicalTime(int logicalTime) {
    this.logicalTime = logicalTime;
  }
  
  public long getTimestamp() {
    return timestamp;
  }
  
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  
  public String getReadableTimestamp() {
    return readableTimestamp;
  }
  
  public void setReadableTimestamp(String readableTimestamp) {
    this.readableTimestamp = readableTimestamp;
  }
  
  public String getInodeName() {
    return inodeName;
  }
  
  public void setInodeName(String inodeName) {
    this.inodeName = inodeName;
  }
  
  public String getXattrName() {
    return xattrName;
  }
  
  public void setXattrName(String xattrName) {
    this.xattrName = xattrName;
  }
  
  public static class TimestampComparator implements Comparator<ProvFileOpHit> {
  
    @Override
    public int compare(ProvFileOpHit o1, ProvFileOpHit o2) {
      int result = Ints.compare(o1.logicalTime, o2.logicalTime);
      if(result == 0) {
        result = Longs.compare(o1.timestamp, o2.timestamp);
      }
      return result;
    }
  }
}
