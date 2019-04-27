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

import io.hops.hopsworks.common.dao.hdfs.inode.Inode;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ProvDatasetState {
  private String name;
  private long inodeId;
  private Inode.MetaStatus metaStatus;
  
  public ProvDatasetState() {}
  
  public ProvDatasetState(String name, long inodeId, Inode.MetaStatus metaStatus) {
    this.name = name;
    this.inodeId = inodeId;
    this.metaStatus = metaStatus;
  }
  
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public long getInodeId() {
    return inodeId;
  }
  
  public void setInodeId(long inodeId) {
    this.inodeId = inodeId;
  }
  
  public Inode.MetaStatus getMetaStatus() {
    return metaStatus;
  }
  
  public void setMetaStatus(Inode.MetaStatus metaStatus) {
    this.metaStatus = metaStatus;
  }
}
