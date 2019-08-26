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
package io.hops.hopsworks.common.provenance.v2;

import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;

import java.util.EnumSet;
import java.util.logging.Level;

public class ProvElastic {
  public interface Field {
  }
  
  public enum FileBase implements Field {
    PROJECT_I_ID,
    INODE_ID,
    INODE_NAME,
    USER_ID,
    APP_ID
  }
  
  public enum FileAux implements Field {
    DATASET_I_ID,
    PARENT_I_ID,
    PARTITION_ID,
    ENTRY_TYPE
  }
  
  public enum FileStateBase implements Field {
    CREATE_TIMESTAMP,
    ML_TYPE,
    ML_ID
  }
  
  public enum FileStateAux implements Field {
    PROJECT_NAME,
    R_CREATE_TIMESTAMP;
  }
  
  public enum FileOpsBase implements Field {
    INODE_OPERATION,
    TIMESTAMP
  }
  
  public enum FileOpsAux implements Field {
    ML_TYPE,
    ML_ID,
    LOGICAL_TIME,
    R_TIMESTAMP,
    INODE_PATH,
    XATTR
  }
  
  public enum AppState implements Field {
    APP_STATE,
    APP_ID
  }
  
  public enum EntryType {
    STATE,
    OPERATION
  }
  
  public static Field extractFileStateQueryResultFields(String val) throws GenericException {
    try {
      return FileBase.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileStateBase.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileStateAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    
    StringBuilder supported = new StringBuilder();
    supported.append(EnumSet.allOf(FileBase.class));
    supported.append(EnumSet.allOf(FileAux.class));
    supported.append(EnumSet.allOf(FileStateBase.class));
    supported.append(EnumSet.allOf(FileStateAux.class));
    throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
      "file state items - param" + val + " not supported - supported:" + supported,
      "exception extracting SortBy param");
  }
  
  public static Field extractFileOpsQueryResultFields(String val) throws GenericException {
    try {
      return FileBase.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileOpsBase.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileOpsAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    
    StringBuilder supported = new StringBuilder();
    supported.append(EnumSet.allOf(FileBase.class));
    supported.append(EnumSet.allOf(FileAux.class));
    supported.append(EnumSet.allOf(FileOpsBase.class));
    supported.append(EnumSet.allOf(FileOpsAux.class));
    throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
      "file ops items - param" + val + " not supported - supported:" + supported,
      "exception extracting SortBy param");
  }
}
