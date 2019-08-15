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

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collection;

public class FileStateResult {
  @XmlRootElement
  public static class List {
    protected Collection<FileState> result;
    
    public List() {}
    
    public List(Collection<FileState> result) {
      this.result = result;
    }
    
    public Collection<FileState> getResult() {
      return result;
    }
    
    public void setResult(Collection<FileState> result) {
      this.result = result;
    }
  }
  
  @XmlRootElement
  public static class MinTree {
    protected Collection<FileStateTree> result;
    
    public MinTree() {}
    
    public MinTree(Collection<FileStateTree> result) {
      this.result = result;
    }
    
    public Collection<FileStateTree> getResult() {
      return result;
    }
    
    public void setResult(Collection<FileStateTree> result) {
      this.result = result;
    }
  }
  
  @XmlRootElement
  public static class FullTree {
    protected Collection<FileStateTree> result;
    protected Collection<FileStateTree> incomplete;
    
    public FullTree() {}
    
    public FullTree(Collection<FileStateTree> result, Collection<FileStateTree> incomplete) {
      this.result = result;
      this.incomplete = incomplete;
    }
    
    public Collection<FileStateTree> getResult() {
      return result;
    }
    
    public void setResult(Collection<FileStateTree> result) {
      this.result = result;
    }
  
    public Collection<FileStateTree> getIncomplete() {
      return incomplete;
    }
  
    public void setIncomplete(Collection<FileStateTree> incomplete) {
      this.incomplete = incomplete;
    }
  }
}