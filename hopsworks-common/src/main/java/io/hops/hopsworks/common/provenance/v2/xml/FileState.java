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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.annotation.XmlRootElement;

import io.hops.hopsworks.common.provenance.MLAssetAppState;
import io.hops.hopsworks.common.provenance.ProvElastic;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import org.elasticsearch.search.SearchHit;

@XmlRootElement
public class FileState implements Comparator<FileState>, ProvenanceController.BasicFileState {
  
  private static final Logger LOG = Logger.getLogger(FileState.class.getName());
  private String id;
  private float score;
  private Map<String, Object> map;
  
  private Long inodeId;
  private String appId;
  private Integer userId;
  private Long projectInodeId;
  private String inodeName;
  private String projectName;
  private String mlType;
  private String mlId;
  private Long createTime;
  private String readableCreateTime;
  private Map<String, String> xattrs = new HashMap<>();
  private MLAssetAppState appState;
  private String fullPath;
  private Long partitionId;
  private Long parentInodeId;
  
  public FileState(){
  }
  
  public FileState(SearchHit hit) {
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
        case ProvElastic.Common.USER_ID_FIELD:
          this.userId = ((Number) entry.getValue()).intValue();
          break;
        case ProvElastic.Common.PROJECT_INODE_ID_FIELD:
          this.projectInodeId = ((Number) entry.getValue()).longValue();
          break;
        case ProvElastic.Common.PARENT_INODE_ID_FIELD :
          this.parentInodeId = ((Number) entry.getValue()).longValue();
          break;
        case ProvElastic.Common.INODE_NAME_FIELD:
          this.inodeName = entry.getValue().toString();
          break;
        case ProvElastic.Common.PROJECT_NAME_FIELD:
          this.projectName = entry.getValue().toString();
          break;
        case ProvElastic.Common.PARTITION_ID :
          this.partitionId = ((Number) entry.getValue()).longValue();
          break;
        case ProvElastic.ML.ML_TYPE:
          this.mlType = entry.getValue().toString();
          break;
        case ProvElastic.ML.ML_ID:
          this.mlId = entry.getValue().toString();
          break;
        case ProvElastic.State.CREATE_TIMESTAMP_FIELD:
          this.createTime = ((Number) entry.getValue()).longValue();
          break;
        case ProvElastic.State.READABLE_CREATE_TIMESTAMP_FIELD:
          this.readableCreateTime = entry.getValue().toString();
          break;
        case ProvElastic.Common.DATASET_INODE_ID_FIELD:
        case ProvElastic.Common.ENTRY_TYPE_FIELD:
          break;
        default:
          if(entry.getValue() == null) {
            LOG.log(Level.WARNING, "empty key:{0}", new Object[]{entry.getKey()});
          } else {
            if(entry.getValue() instanceof Map) {
              try {
                Map<String, Object> e = (Map<String, Object>) entry.getValue();
                if(e.containsKey("raw")) {
                  String xattrKey = entry.getKey();
                  String xattrVal = (String) e.get("raw");
                  xattrs.put(xattrKey, xattrVal);
                }
              } catch (ClassCastException e) {
                break;
              }
            }
            String value = entry.getValue().toString();
            if (!value.equals("")) {
            
            }
          }
          break;
      }
    }
  }

  public float getScore() {
    return score;
  }
  
  @Override
  public int compare(FileState o1, FileState o2) {
    return Float.compare(o2.getScore(), o1.getScore());
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
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

  @Override
  public Long getInodeId() {
    return inodeId;
  }

  public void setInodeId(long inodeId) {
    this.inodeId = inodeId;
  }

  public String getAppId() {
    return appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public int getUserId() {
    return userId;
  }

  public void setUserId(int userId) {
    this.userId = userId;
  }

  @Override
  public Long getProjectInodeId() {
    return projectInodeId;
  }

  public void setProjectInodeId(Long projectInodeId) {
    this.projectInodeId = projectInodeId;
  }

  @Override
  public String getInodeName() {
    return inodeName;
  }

  public void setInodeName(String inodeName) {
    this.inodeName = inodeName;
  }

  public String getMlType() {
    return mlType;
  }

  public void setMlType(String mlType) {
    this.mlType = mlType;
  }

  public String getMlId() {
    return mlId;
  }

  public void setMlId(String mlId) {
    this.mlId = mlId;
  }

  public Long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Long createTime) {
    this.createTime = createTime;
  }

  public String getReadableCreateTime() {
    return readableCreateTime;
  }

  public void setReadableCreateTime(String readableCreateTime) {
    this.readableCreateTime = readableCreateTime;
  }

  public Map<String, String> getXattrs() {
    return xattrs;
  }

  public void setXattrs(Map<String, String> xattrs) {
    this.xattrs = xattrs;
  }

  public MLAssetAppState getAppState() {
    return appState;
  }

  public void setAppState(MLAssetAppState appState) {
    this.appState = appState;
  }
  
  public String getFullPath() {
    return fullPath;
  }
  
  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
  }
  
  public Long getPartitionId() {
    return partitionId;
  }
  
  public void setPartitionId(Long partitionId) {
    this.partitionId = partitionId;
  }
  
  @Override
  public Long getParentInodeId() {
    return parentInodeId;
  }
  
  public void setParentInodeId(Long parentInodeId) {
    this.parentInodeId = parentInodeId;
  }
  
  public void setInodeId(Long inodeId) {
    this.inodeId = inodeId;
  }
  
  public void setUserId(Integer userId) {
    this.userId = userId;
  }
  
  public String getProjectName() {
    return projectName;
  }
  
  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }
  
  @Override
  public boolean isProject() {
    return projectInodeId == inodeId;
  }
}
