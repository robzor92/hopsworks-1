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

import javax.xml.bind.annotation.XmlRootElement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@XmlRootElement
public class ProvFileSummaryOpsHit {
  private long inodeId;
  private String appId;
  private String inodeName;
  private Set<String> ops = new HashSet<>();
  
  public ProvFileSummaryOpsHit() {}
  
  private ProvFileSummaryOpsHit(long inodeId, String appId, String inodeName) {
    this.inodeId = inodeId;
    this.appId = appId;
    this.inodeName = inodeName;
  }
  
  public long getInodeId() {
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
  
  public Set<String> getOps() {
    return ops;
  }
  
  public void setOps(Set<String> ops) {
    this.ops = ops;
  }
  
  public void addOp(String op) {
    ops.add(op);
  }
  
  public String getInodeName() {
    return inodeName;
  }
  
  public void setInodeName(String inodeName) {
    this.inodeName = inodeName;
  }
  
  public static List<ProvFileSummaryOpsHit> summary(List<ProvFileOpHit> fileOps) {
    Map<Long, ProvFileSummaryOpsHit> files = new HashMap<>();
    for(ProvFileOpHit fileOp : fileOps) {
      ProvFileSummaryOpsHit file = files.get(fileOp.getInodeId());
      if(file == null) {
        file = new ProvFileSummaryOpsHit(fileOp.getInodeId(), fileOp.getAppId(), fileOp.getInodeName());
        files.put(fileOp.getInodeId(), file);
      }
      file.addOp(fileOp.getInodeOperation());
    }
    return new LinkedList<>(files.values());
  }
}
