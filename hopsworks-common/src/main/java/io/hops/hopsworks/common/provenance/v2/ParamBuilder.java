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

import org.javatuples.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ParamBuilder {
  public static void addToFilters(Map<String, List<Pair<ProvQuery.Field, Object>>> filters,
    Pair<ProvQuery.Field, Object> filter) {
    List<Pair<ProvQuery.Field, Object>> fieldFilters = filters.get(filter.getValue0().queryFieldName());
    if(fieldFilters == null) {
      fieldFilters = new LinkedList<>();
      filters.put(filter.getValue0().queryFieldName(), fieldFilters);
    }
    fieldFilters.add(filter);
  }
}
