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

public class ProvElastic {
  public static class Common {
    public static final String PROJECT_INODE_ID_FIELD = "project_i_id";
    public static final String INODE_ID_FIELD = "inode_id";
    public static final String INODE_OPERATION_FIELD = "inode_operation";
    public static final String APP_ID_FIELD = "io_app_id";
    public static final String LOGICAL_TIME_FIELD = "io_logical_time";
    public static final String TIMESTAMP_FIELD = "io_timestamp";
    public static final String READABLE_TIMESTAMP_FIELD = "i_readable_t";
    public static final String INODE_NAME_FIELD = "i_name";
    public static final String XATTR_NAME_FIELD = "xattr";
    public static final String ENTRY_TYPE_FIELD = "entry_type";
  }
}
