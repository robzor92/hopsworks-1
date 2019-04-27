/*
 * Changes to this file committed after and not including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
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
 *
 * Changes to this file committed before and including commit-id: ccc0d2c5f9a5ac661e60e6eaf138de7889928b8b
 * are released under the following license:
 *
 * Copyright (C) 2013 - 2018, Logical Clocks AB and RISE SICS AB. All rights reserved
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS  OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package io.hops.hopsworks.common.provenance;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.annotation.XmlRootElement;
import org.elasticsearch.search.SearchHit;

@XmlRootElement
public class FProvMLAssetHit implements Comparator<FProvMLAssetHit> {
  private static final Logger LOG = Logger.getLogger(FProvMLAssetHit.class.getName());
  private String id;
  private float score;
  private Map<String, Object> map;
  
  private long inode_id;
  private String creator_app_id;
  private int creator_user_id;
  private long project_inode_id;
  private String inode_name;
  private String create_timestamp;
  private String mlType;
  private String mlId;
  private Map<String, String> xattrs = new HashMap<>();
  
  public FProvMLAssetHit(){
  }
  
  public FProvMLAssetHit(SearchHit hit) {
    this.id = hit.getId();
    this.score = hit.getScore();
    //the source of the retrieved record (i.e. all the indexed information)
    this.map = hit.getSourceAsMap();

    //export the name of the retrieved record from the list
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      //set the name explicitly so that it's easily accessible in the frontend
      switch (entry.getKey()) {
        case FileProvenanceHit.INODE_ID_FIELD:
          this.inode_id = ((Number) entry.getValue()).longValue();
          break;
        case FileProvenanceHit.APP_ID_FIELD:
          this.creator_app_id = entry.getValue().toString();
          break;
        case FileProvenanceHit.USER_ID_FIELD:
          this.creator_user_id = ((Number) entry.getValue()).intValue();
          break;
        case FileProvenanceHit.PROJECT_INODE_ID_FIELD:
          this.project_inode_id = ((Number) entry.getValue()).longValue();
          break;
        case FileProvenanceHit.INODE_NAME_FIELD:
          this.inode_name = entry.getValue().toString();
          break;
        case FileProvenanceHit.TIMESTAMP_FIELD:
          this.create_timestamp = entry.getValue().toString();
          break;
        case FileProvenanceHit.ML_TYPE_FIELD:
          this.mlType = entry.getValue().toString();
          break;
        case FileProvenanceHit.ML_ID_FIELD:
          this.mlId = entry.getValue().toString();
          break;
        case FileProvenanceHit.ALIVE:
          break;
        default:
          if(entry.getValue() == null) {
            LOG.log(Level.WARNING, "empty key:{0}", new Object[]{entry.getKey()});
          } else {
            xattrs.put(entry.getKey(), entry.getValue().toString());
          }
          break;
      }
    }
  }

  public float getScore() {
    return score;
  }
  
  @Override
  public int compare(FProvMLAssetHit o1, FProvMLAssetHit o2) {
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

  public long getInode_id() {
    return inode_id;
  }

  public void setInode_id(long inode_id) {
    this.inode_id = inode_id;
  }

  public String getCreator_app_id() {
    return creator_app_id;
  }

  public void setCreator_app_id(String creator_app_id) {
    this.creator_app_id = creator_app_id;
  }

  public int getCreator_user_id() {
    return creator_user_id;
  }

  public void setCreator_user_id(int creator_user_id) {
    this.creator_user_id = creator_user_id;
  }

  public long getProject_inode_id() {
    return project_inode_id;
  }

  public void setProject_inode_id(long project_inode_id) {
    this.project_inode_id = project_inode_id;
  }

  public String getInode_name() {
    return inode_name;
  }

  public void setInode_name(String inode_name) {
    this.inode_name = inode_name;
  }

  public String getCreate_timestamp() {
    return create_timestamp;
  }

  public void setCreate_timestamp(String create_timestamp) {
    this.create_timestamp = create_timestamp;
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

  public Map<String, String> getXattrs() {
    return xattrs;
  }

  public void setXattrs(Map<String, String> xattrs) {
    this.xattrs = xattrs;
  }
}
