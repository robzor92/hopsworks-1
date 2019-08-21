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
package io.hops.hopsworks.common.provenance.v2.xml;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.hops.hopsworks.common.provenance.MLAssetAppState;
import io.hops.hopsworks.common.provenance.v2.ProvElastic;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.search.SearchHit;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@XmlRootElement
public class FileOp implements Comparator<FileOp>  {
  private static final Logger LOG = Logger.getLogger(FileOp.class.getName());
  public static final TimestampComparator timestampComparator = new TimestampComparator();
  
  private String id;
  private float score;
  private Map<String, Object> map;
  
  private long inodeId;
  private String inodeOperation;
  private String appId;
  private Integer userId;
  private Long parentInodeId;
  private Long projectInodeId;
  private int logicalTime;
  private long timestamp;
  private String readableTimestamp;
  private String inodeName;
  private String xattrName;
  private String inodePath;
  private Long partitionId;
  private MLAssetAppState appState;
  
  public FileOp() {}

  public static FileOp instance(SearchHit hit) {
    FileOp result = new FileOp();
    result.id = hit.getId();
    result.score = hit.getScore();
    result.map = hit.getSourceAsMap();
  
    for (Map.Entry<String, Object> entry : result.map.entrySet()) {
      try {
        ProvElastic.Field field = ProvElastic.extractFileOpsQueryResultFields(entry.getKey());
        if (field instanceof ProvElastic.FileBase) {
          ProvElastic.FileBase fileBase = (ProvElastic.FileBase) field;
          switch (fileBase) {
            case PROJECT_I_ID:
              result.projectInodeId = ((Number) entry.getValue()).longValue();
              break;
            case INODE_ID:
              result.inodeId = ((Number) entry.getValue()).longValue();
              break;
            case APP_ID:
              result.appId = entry.getValue().toString();
              break;
            case USER_ID:
              result.userId = ((Number) entry.getValue()).intValue();
              break;
            case INODE_NAME:
              result.inodeName = entry.getValue().toString();
              break;
            default:
              throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
                "field:" + field + "not managed in file ops return (1)");
          }
        } else if (field instanceof ProvElastic.FileOpsBase) {
          ProvElastic.FileOpsBase fileOpsBase = (ProvElastic.FileOpsBase) field;
          switch (fileOpsBase) {
            case INODE_OPERATION:
              result.inodeOperation = entry.getValue().toString();
              break;
            case TIMESTAMP:
              result.timestamp = ((Number) entry.getValue()).longValue();
              break;
            default:
              throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
                "field:" + field + "not managed in file ops return (2)");
          }
        } else if (field instanceof ProvElastic.FileAux) {
          ProvElastic.FileAux fileReturn = (ProvElastic.FileAux) field;
          switch (fileReturn) {
            case PARENT_I_ID:
              result.parentInodeId = ((Number) entry.getValue()).longValue();
              break;
            case PARTITION_ID:
              result.partitionId = ((Number) entry.getValue()).longValue();
              break;
            case ENTRY_TYPE:
              break;
            default:
              throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
                "field:" + field + "not managed in file ops return (3)");
          }
        } else if (field instanceof ProvElastic.FileOpsAux) {
          ProvElastic.FileOpsAux fileOpsReturn = (ProvElastic.FileOpsAux) field;
          switch (fileOpsReturn) {
            case ML_ID:
              break;
            case ML_TYPE:
              break;
            case LOGICAL_TIME:
              result.logicalTime = ((Number) entry.getValue()).intValue();
              break;
            case R_TIMESTAMP:
              result.readableTimestamp = entry.getValue().toString();
              break;
            case INODE_PATH:
              result.inodePath = entry.getValue().toString();
              break;
            case XATTR:
              result.xattrName = entry.getValue().toString();
              break;
            default:
              throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
                "field:" + field + "not managed in file ops return (3)");
          }
        } else {
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
            "field:" + field + "not managed in file ops return (4)");
        }
      } catch (GenericException e) {
        LOG.log(Level.WARNING, "file osp - unknown key:{0} value:{1}",
          new Object[]{entry.getKey(), entry.getValue()});
      }
    }
    return result;
  }
  
  @Override
  public int compare(FileOp o1, FileOp o2) {
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
  
  public String getInodePath() {
    return inodePath;
  }
  
  public void setInodePath(String inodePath) {
    this.inodePath = inodePath;
  }
  
  public Long getParentInodeId() {
    return parentInodeId;
  }
  
  public void setParentInodeId(Long parentInodeId) {
    this.parentInodeId = parentInodeId;
  }
  
  public Integer getUserId() {
    return userId;
  }
  
  public void setUserId(Integer userId) {
    this.userId = userId;
  }
  
  public Long getPartitionId() {
    return partitionId;
  }
  
  public void setPartitionId(Long partitionId) {
    this.partitionId = partitionId;
  }
  
  public Long getProjectInodeId() {
    return projectInodeId;
  }
  
  public void setProjectInodeId(Long projectInodeId) {
    this.projectInodeId = projectInodeId;
  }
  
  public MLAssetAppState getAppState() {
    return appState;
  }
  
  public void setAppState(MLAssetAppState appState) {
    this.appState = appState;
  }
  
  public static class TimestampComparator implements Comparator<FileOp> {
  
    @Override
    public int compare(FileOp o1, FileOp o2) {
      int result = Ints.compare(o1.logicalTime, o2.logicalTime);
      if(result == 0) {
        result = Longs.compare(o1.timestamp, o2.timestamp);
      }
      return result;
    }
  }
}
