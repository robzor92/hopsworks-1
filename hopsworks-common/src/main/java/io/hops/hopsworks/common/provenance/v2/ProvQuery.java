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

import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import java.util.EnumSet;
import java.util.StringJoiner;
import java.util.logging.Level;

public class ProvQuery {
  
  public interface ValParser {
    Object parse(String o) throws GenericException;
  }
  
  public interface Field {
    String elasticFieldName();
    String queryFieldName();
    FilterType filterType();
    ValParser filterValParser();
  }
  
  public enum FileState implements Field {
    PROJECT_I_ID(ProvElastic.FileBase.PROJECT_I_ID, new LongValParser()),
    FILE_I_ID(ProvElastic.FileBase.INODE_ID, new LongValParser()),
    FILE_NAME(ProvElastic.FileBase.INODE_NAME, new StringValParser()),
    USER_ID(ProvElastic.FileBase.USER_ID, new IntValParser()),
    APP_ID(ProvElastic.FileBase.APP_ID, new StringValParser()),
    CREATE_TIMESTAMP(ProvElastic.FileStateBase.CREATE_TIMESTAMP, new LongValParser()),
    ML_TYPE(ProvElastic.FileStateBase.ML_TYPE, new MLTypeValParser()),
    ML_ID(ProvElastic.FileStateBase.ML_ID, new StringValParser());
  
    ProvElastic.Field elasticField;
    ValParser filterValParser;
  
    FileState(ProvElastic.Field elasticField, ValParser filterValParser) {
      this.elasticField = elasticField;
      this.filterValParser = filterValParser;
    }
  
    @Override
    public String elasticFieldName() {
      return elasticField.toString().toLowerCase();
    }
  
    @Override
    public String queryFieldName() {
      return name().toLowerCase();
    }
    
    @Override
    public FilterType filterType() {
      return FilterType.EXACT;
    }
  
    @Override
    public ValParser filterValParser() {
      return filterValParser;
    }
  }
  
  public enum FileStateAux implements Field {
    FILE_NAME_LIKE(FileState.FILE_NAME, FilterType.LIKE),
    CREATE_TIMESTAMP_LT(FileState.CREATE_TIMESTAMP, FilterType.RANGE_LT),
    CREATE_TIMESTAMP_GT(FileState.CREATE_TIMESTAMP, FilterType.RANGE_GT),
    CREATETIME(FileState.CREATE_TIMESTAMP, FilterType.EXACT);
  
    FileState base;
    FilterType filterType;
  
    FileStateAux(FileState base, FilterType filterType) {
      this.base = base;
      this.filterType = filterType;
    }
  
    @Override
    public String elasticFieldName() {
      return base.elasticFieldName();
    }
  
    @Override
    public String queryFieldName() {
      return base.elasticFieldName();
    }
  
    @Override
    public FilterType filterType() {
      return filterType;
    }
  
    @Override
    public ValParser filterValParser() {
      return base.filterValParser();
    }
  }
  
  public enum FileOps implements Field {
    PROJECT_I_ID(ProvElastic.FileBase.PROJECT_I_ID, new LongValParser()),
    FILE_I_ID(ProvElastic.FileBase.INODE_ID, new LongValParser()),
    FILE_NAME(ProvElastic.FileBase.INODE_NAME, new StringValParser()),
    USER_ID(ProvElastic.FileBase.USER_ID, new IntValParser()),
    APP_ID(ProvElastic.FileBase.APP_ID, new StringValParser()),
    FILE_OPERATION(ProvElastic.FileOpsBase.INODE_OPERATION, new FileOpValParser()),
    TIMESTAMP(ProvElastic.FileOpsBase.TIMESTAMP, new LongValParser());
    
    ProvElastic.Field elasticField;
    ValParser valParser;
  
    FileOps(ProvElastic.Field elasticField, ValParser valParser) {
      this.elasticField = elasticField;
      this.valParser = valParser;
    }
  
    @Override
    public String elasticFieldName() {
      return elasticField.toString().toLowerCase();
    }
  
    @Override
    public String queryFieldName() {
      return name().toLowerCase();
    }
    
    @Override
    public FilterType filterType() {
      return FilterType.EXACT;
    }
  
    @Override
    public ValParser filterValParser() {
      return valParser;
    }
  }
  
  public enum FileOpsAux implements Field {
    FILE_NAME_LIKE(FileOps.FILE_NAME, FilterType.LIKE),
    TIMESTAMP_LT(FileOps.TIMESTAMP, FilterType.RANGE_LT),
    TIMESTAMP_GT(FileOps.TIMESTAMP, FilterType.RANGE_GT);
  
    FileOps base;
    FilterType filterType;
  
    FileOpsAux(FileOps base, FilterType filterType) {
      this.base = base;
      this.filterType = filterType;
    }
  
    @Override
    public String elasticFieldName() {
      return base.elasticFieldName();
    }
  
    @Override
    public String queryFieldName() {
      return base.elasticFieldName();
    }
    
    @Override
    public FilterType filterType() {
      return filterType;
    }
  
    @Override
    public ValParser filterValParser() {
      return base.filterValParser();
    }
  }
  
  public enum ExpansionApp implements Field {
    APP_STATE(ProvElastic.AppState.APP_STATE, new AppStateValParser()),
    APP_ID(ProvElastic.AppState.APP_ID, new StringValParser());
    
    public final ProvElastic.AppState elasticField;
    public final ValParser valParser;
    
    ExpansionApp(ProvElastic.AppState elasticField, ValParser valParser) {
      this.elasticField = elasticField;
      this.valParser = valParser;
    }
    
    @Override
    public String elasticFieldName() {
      return elasticField.toString().toLowerCase();
    }
  
    @Override
    public String queryFieldName() {
      return name().toLowerCase();
    }
    
    @Override
    public FilterType filterType() {
      return FilterType.EXACT;
    }
    
    @Override
    public ValParser filterValParser() {
      return valParser;
    }
  }
  
  public static Pair<Field, Object> extractFilter(String param, QueryType queryType)
    throws GenericException {
    String rawFilter;
    String rawVal;
    if (param.contains(":")) {
      int aux = param.indexOf(':');
      rawFilter = param.substring(0, aux);
      rawVal = param.substring(aux+1);
    } else {
      rawFilter = param;
      rawVal = "true";
    }
    Field field;
    switch(queryType) {
      case QUERY_FILE_STATE: field = extractFileStateQueryFilterFields(rawFilter); break;
      case QUERY_FILE_OP: field = extractFileOpsQueryFilterFields(rawFilter); break;
      case QUERY_EXPANSION_APP: field = extractExpansionAppQueryBaseFields(rawFilter); break;
      default:
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "unknown query type:" + queryType, "exception extracting Filter");
    }
    Object val = field.filterValParser().parse(rawVal);
    return Pair.with(field, val);
  }
  
  public static Pair<Field, SortOrder> extractSort(String param, QueryType queryType)
    throws GenericException {
    String rawSortField;
    String rawSortOrder;
    if (param.contains(":")) {
      int aux = param.indexOf(':');
      rawSortField = param.substring(0, aux);
      rawSortOrder = param.substring(aux+1);
    } else {
      rawSortField = param;
      rawSortOrder = "ASC";
    }
    Field sortField = extractBaseField(rawSortField, queryType);
    SortOrder sortOrder = extractSortOrder(rawSortOrder);
    return Pair.with(sortField, sortOrder);
  }
  
  public static Field extractBaseField(String rawField, QueryType queryType) throws GenericException {
    Field field;
    switch(queryType) {
      case QUERY_FILE_STATE: field = extractFileStateQueryBaseFields(rawField); break;
      case QUERY_FILE_OP: field = extractFileOpsQueryBaseFields(rawField); break;
      case QUERY_EXPANSION_APP: field = extractExpansionAppQueryBaseFields(rawField); break;
      default:
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "unknown field:" + queryType);
    }
    return field;
  }
  
  public static Field extractFileStateQueryFilterFields(String val) throws GenericException {
    try {
      return FileState.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
    }
    try {
      return FileStateAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
    }
    StringBuilder supported = new StringBuilder();
    supported.append(EnumSet.allOf(FileState.class));
    supported.append(EnumSet.allOf(FileStateAux.class));
    throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
      "filter param" + val + " not supported - supported:" + supported,
      "exception extracting SortBy param");
  }
  
  public static Field extractFileStateQueryBaseFields(String val) throws GenericException {
    try {
      return FileState.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    
    StringBuilder supported = new StringBuilder();
    supported.append(EnumSet.allOf(FileState.class));
    throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
      "sort param" + val + " not supported - supported:" + supported,
      "exception extracting SortBy param");
  }
  
  
  
  public static SortOrder extractSortOrder(String val) throws GenericException {
    try{
      return SortOrder.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "sort order " + val + " not supported - supported order:" + EnumSet.allOf(SortOrder.class),
        "exception extracting FilterBy param", e);
    }
  }
  
  public static Field extractFileOpsQueryFilterFields(String val) throws GenericException {
    try {
      return FileOps.valueOf(val.toUpperCase());
    } catch (NullPointerException | IllegalArgumentException e) {
      //try next
    }
    try {
      return FileOpsAux.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException ee) {
      //try next
    }
    StringBuilder supported = new StringBuilder();
    supported.append(EnumSet.allOf(FileOps.class));
    supported.append(EnumSet.allOf(FileOpsAux.class));
    throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
      "filter param" + val + " not supported - supported:" + supported,
      "exception extracting SortBy param");
  }
  
  public static Field extractFileOpsQueryBaseFields(String val) throws GenericException {
    try {
      return FileOps.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      //try next
    }
    StringBuilder supported = new StringBuilder();
    supported.append(EnumSet.allOf(FileOps.class));
    throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
      "sort param" + val + " not supported - supported:" + supported,
      "exception extracting SortBy param");
  }
  
  public static Field extractExpansionAppQueryBaseFields(String val) throws GenericException {
    try {
      return ExpansionApp.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      StringBuilder supported = new StringBuilder();
      supported.append(EnumSet.allOf(ExpansionApp.class));
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "sort param" + val + " not supported - supported:" + supported,
        "exception extracting SortBy param", e);
    }
  }
  
  public static Pair<String, String> extractXAttrParam(String param) throws GenericException {
    String[] xattrParts = param.split(":");
    if(xattrParts.length != 2 || xattrParts[0].isEmpty()) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "malformed xattr:" + param);
    }
    return Pair.with(processXAttrKey(xattrParts[0]), xattrParts[1]);
  }
  
  public static String processXAttrKey(String key) {
    String[] keyParts =key.split("\\.");
    StringJoiner keyj = new StringJoiner(".");
    if(keyParts.length == 1) {
      keyj.add(keyParts[0]).add("raw");
    } else {
      keyj.add(keyParts[0]).add("value");
      for(int i = 1; i < keyParts.length; i++) keyj.add(keyParts[i]);
    }
    return keyj.toString();
  }
  
  public static String processXAttrKeyAsKeyword(String key) {
    return processXAttrKey(key) + ".keyword";
  }
  
  public enum FileExpansions {
    APP("APP");
    
    public final String queryParamName;
    
    FileExpansions(String queryParamName) {
      this.queryParamName = queryParamName;
    }
    
    @Override
    public String toString() {
      return queryParamName;
    }
  }
  
  public enum FilterType {
    EXACT,
    LIKE,
    RANGE_LT,
    RANGE_GT;
  }
  
  public enum QueryType {
    QUERY_FILE_STATE,
    QUERY_FILE_OP,
    QUERY_EXPANSION_APP;
  }
  
  public static class IntValParser implements ValParser {
    
    @Override
    public Integer parse(String o) throws IllegalArgumentException {
      try {
        return Integer.valueOf(o);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("expected int - found " + o, e);
      }
    }
  }
  
  public static class StringValParser implements ValParser {
    
    @Override
    public String parse(String o) {
      return o;
    }
  }
  
  public static class LongValParser implements ValParser {
    
    @Override
    public Long parse(String o) throws IllegalArgumentException {
      try {
        return Long.valueOf(o);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("expected long - found " + o, e);
      }
    }
  }
  
  public static class MLTypeValParser implements ValParser {
    @Override
    public String parse(String o) throws IllegalArgumentException {
      try {
        return Provenance.MLType.valueOf(o).name();
      } catch (NullPointerException | IllegalArgumentException e) {
        throw new IllegalArgumentException("expected:" + EnumSet.allOf(Provenance.MLType.class) + "found " + o, e);
      }
    }
  }
  
  public static class AppStateValParser implements ValParser {
    @Override
    public String parse(String o) throws IllegalArgumentException {
      try {
        return Provenance.AppState.valueOf(o).name();
      } catch (NullPointerException | IllegalArgumentException e) {
        throw new IllegalArgumentException("expected:" + EnumSet.allOf(Provenance.AppState.class) + "found " + o, e);
      }
    }
  }
  
  public static class FileOpValParser implements ValParser {
    
    @Override
    public String parse(String o) throws IllegalArgumentException {
      try {
        return ProvFileOps.valueOf(o).name();
      } catch (NullPointerException | IllegalArgumentException e) {
        throw new IllegalArgumentException("expected:" + EnumSet.allOf(ProvFileOps.class) + "found " + o, e);
      }
    }
  }
}
