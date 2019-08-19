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

import io.hops.hopsworks.common.provenance.v2.ProvFileOps;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import java.util.EnumSet;
import java.util.StringJoiner;
import java.util.logging.Level;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

public class ProvElastic {
  
  public interface ElasticFilters {
    String paramName();
    String elasticField();
    FilterType filterType();
  }
  
  public enum FilterType {
    EXACT,
    LIKE,
    RANGE_LT,
    RANGE_GT;
  }
  
  public interface ElasticSortBy {
    String paramName();
    String elasticField();
  }
  
  public interface FilterValBuilder {
    Object build(String o) throws GenericException;
  }
  
  public static class StringFilterVal implements FilterValBuilder {
  
    @Override
    public String build(String o) {
      return o;
    }
  }
  
  public static class IntFilterVal implements FilterValBuilder {
    
    @Override
    public Integer build(String o) throws IllegalArgumentException {
      try {
        return Integer.valueOf(o);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("expected int - found " + o, e);
      }
    }
  }
  
  public static class LongFilterVal implements FilterValBuilder {
    
    @Override
    public Long build(String o) throws IllegalArgumentException {
      try {
        return Long.valueOf(o);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("expected long - found " + o, e);
      }
    }
  }
  
  public static class FileOpFilterVal implements FilterValBuilder {
  
    @Override
    public String build(String o) throws IllegalArgumentException {
      try {
        return ProvFileOps.valueOf(o).name();
      } catch (NullPointerException | IllegalArgumentException e) {
        throw new IllegalArgumentException("expected:" + EnumSet.allOf(ProvFileOps.class) + "found " + o, e);
      }
    }
  }
  
  public enum FileOpsFilter implements ElasticFilters {
    PROJECT_I_ID("PROJECT_I_ID", Common.PROJECT_INODE_ID_FIELD, FilterType.EXACT, new LongFilterVal()),
    FILE_INODE_ID("FILE_INODE_ID", Common.INODE_ID_FIELD, FilterType.EXACT, new LongFilterVal()),
    FILE_NAME("FILE_NAME", Common.INODE_NAME_FIELD, FilterType.EXACT, new StringFilterVal()),
    FILE_NAME_LIKE("FILE_NAME_LIKE", Common.INODE_NAME_FIELD, FilterType.LIKE, new StringFilterVal()),
    FILE_OPERATION("FILE_OPERATION", Op.INODE_OPERATION_FIELD, FilterType.EXACT, new FileOpFilterVal()),
    USER_ID("USER_ID", Common.USER_ID_FIELD, FilterType.EXACT, new IntFilterVal()),
    APP_ID("APP_ID", Common.APP_ID_FIELD, FilterType.EXACT, new StringFilterVal()),
    TIMESTAMP_LT("TIMESTAMP_LT", Op.TIMESTAMP_FIELD, FilterType.RANGE_LT, new LongFilterVal()),
    TIMESTAMP_GT("TIMESTAMP_GT", Op.TIMESTAMP_FIELD, FilterType.RANGE_GT, new LongFilterVal());
    public final String queryParamName;
    public final String elasticParamName;
    public final FilterType filterType;
    public final FilterValBuilder valBuilder;
    
    FileOpsFilter(String queryParamName, String elasticParamName, FilterType filterType, FilterValBuilder valBuilder) {
      this.queryParamName = queryParamName;
      this.elasticParamName = elasticParamName;
      this.filterType = filterType;
      this.valBuilder = valBuilder;
    }
  
    @Override
    public String toString() {
      return queryParamName;
    }
    
    @Override
    public String paramName() {
      return queryParamName;
    }
    
    @Override
    public String elasticField() {
      return elasticParamName;
    }
  
    @Override
    public FilterType filterType() {
      return filterType;
    }
  }
  
  public enum FileStateExpansions {
    APP_STATE("APP_STATE");
    
    public final String queryParamName;
    
    FileStateExpansions(String queryParamName) {
      this.queryParamName = queryParamName;
    }
    
    @Override
    public String toString() {
      return queryParamName;
    }
  }
  
  public enum FileStateFilter implements ElasticFilters {
    PROJECT_I_ID("PROJECT_I_ID", Common.PROJECT_INODE_ID_FIELD, FilterType.EXACT),
    INODE_ID("INODE_ID", Common.INODE_ID_FIELD, FilterType.EXACT),
    FILE_NAME("FILE_NAME", Common.INODE_NAME_FIELD, FilterType.EXACT),
    FILE_NAME_LIKE("FILE_NAME_LIKE", Common.INODE_NAME_FIELD, FilterType.LIKE),
    USER_ID("USER_ID", Common.USER_ID_FIELD, FilterType.EXACT),
    APP_ID("APP_ID", Common.APP_ID_FIELD, FilterType.EXACT),
    CREATE_TIMESTAMP_LT("CREATE_TIMESTAMP_LT", State.CREATE_TIMESTAMP_FIELD, FilterType.RANGE_LT),
    CREATE_TIMESTAMP_GT("CREATE_TIMESTAMP_GT", State.CREATE_TIMESTAMP_FIELD, FilterType.RANGE_GT),
    ML_TYPE("ML_TYP", ML.ML_TYPE, FilterType.EXACT),
    ML_ID("ML_ID", ML.ML_ID, FilterType.EXACT);
    public final String queryParamName;
    public final String elasticParamName;
    public final FilterType filterType;
  
    FileStateFilter(String queryParamName, String elasticParamName, FilterType filterType) {
      this.queryParamName = queryParamName;
      this.elasticParamName = elasticParamName;
      this.filterType = filterType;
    }
    
    @Override
    public String toString() {
      return queryParamName;
    }
    
    @Override
    public String elasticField() {
      return elasticParamName;
    }
  
    @Override
    public String paramName() {
      return queryParamName;
    }
    
    @Override
    public FilterType filterType() {
      return filterType;
    }
  }
  
  public enum FileStateSortBy implements ElasticSortBy {
    CREATE_TIMESTAMP("CREATE_TIMESTAMP", State.CREATE_TIMESTAMP_FIELD);
    private final String queryParamName;
    private final String elasticParamName;
  
    FileStateSortBy(String queryParamName, String elasticParamName) {
      this.queryParamName = queryParamName;
      this.elasticParamName = elasticParamName;
    }
    
    @Override
    public String toString() {
      return queryParamName;
    }
    
    @Override
    public String elasticField() {
      return elasticParamName;
    }
    
    @Override
    public String paramName() {
      return queryParamName;
    }
  }
  
  public enum AppStateFilter implements ElasticFilters {
    APP_ID("APP_ID", "app_id", FilterType.EXACT),
    APP_STATE("APP_STATE", "app_state", FilterType.EXACT);
  
    public final String queryParamName;
    public final String elasticParamName;
    public final FilterType type;
  
    AppStateFilter(String queryParamName, String elasticParamName, FilterType type) {
      this.queryParamName = queryParamName;
      this.elasticParamName = elasticParamName;
      this.type = type;
    }
  
    @Override
    public String toString() {
      return queryParamName;
    }
  
    @Override
    public String elasticField() {
      return elasticParamName;
    }
    
    @Override
    public String paramName() {
      return queryParamName;
    }
  
    @Override
    public FilterType filterType() {
      return type;
    }
  }
  
  public static Pair<ProvElastic.FileStateFilter, Object> extractFileStateFilterBy(String param)
    throws GenericException {
    if (param.contains(":")) {
      int aux = param.indexOf(':');
      String rawFilter = param.substring(0, aux);
      String filterVal = param.substring(aux+1);
      FileStateFilter filter = processFilterField(rawFilter);
      return Pair.with(filter, filterVal);
    } else {
      FileStateFilter filter = processFilterField(param);
      return Pair.with(filter, true);
    }
  }
  
  private static FileStateFilter processFilterField(String val) throws GenericException {
    try {
      return ProvElastic.FileStateFilter.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "filter param " + val + " not supported - supported:" + EnumSet.allOf(FileStateFilter.class),
        "exception extracting FilterBy param", e);
    }
  }
  
  public static Pair<ProvElastic.FileStateSortBy, SortOrder> extractFileStateSortBy(String param)
    throws GenericException {
    if (param.contains(":")) {
      int aux = param.indexOf(':');
      String rawSortField = param.substring(0, aux);
      String rawSortOrder = param.substring(aux+1);
      FileStateSortBy sortField = processSortField(rawSortField);
      SortOrder sortOrder = processSortOrder(rawSortOrder);
      return Pair.with(sortField, sortOrder);
    } else {
      ProvElastic.FileStateSortBy sortField = processSortField(param);
      return Pair.with(sortField, SortOrder.ASC);
    }
  }
  
  private static FileStateSortBy processSortField(String val) throws GenericException {
    try {
      return FileStateSortBy.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "sort param" + val + " not supported - supported:" + EnumSet.allOf(FileStateSortBy.class),
        "exception extracting SortBy param", e);
    }
  }
  
  private static SortOrder processSortOrder(String val) throws GenericException {
    try{
      return SortOrder.valueOf(val.toUpperCase());
    } catch(NullPointerException | IllegalArgumentException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "sort order " + val + " not supported - supported order:" + EnumSet.allOf(SortOrder.class),
        "exception extracting FilterBy param", e);
    }
  }
  
  public static Pair<FileOpsFilter, Object> extractFileOpsParam(String param) throws GenericException {
    if (param.contains(":")) {
      int aux = param.indexOf(':');
      String rawParamName = param.substring(0, aux);
      String rawParamVal = param.substring(aux+1);
      try {
        FileOpsFilter filter = FileOpsFilter.valueOf(rawParamName.toUpperCase());
        Object paramVal = filter.valBuilder.build(rawParamVal);
        return Pair.with(filter, paramVal);
      } catch(NullPointerException | IllegalArgumentException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "param " + param.substring(0, aux) + " not supported - supported params:"
            + EnumSet.allOf(FileOpsFilter.class),
          "exception extracting FilterBy param", e);
      }
     
    } else {
      FileOpsFilter filter = FileOpsFilter.valueOf(param.toUpperCase());
      return Pair.with(filter, true);
    }
  }
  
  public static Pair<AppStateFilter, Object> extractAppStateParam(String param) throws GenericException {
    if (param.contains(":")) {
      int aux = param.indexOf(':');
      String rawParamName = param.substring(0, aux);
      String paramVal = param.substring(aux+1);
      try {
        AppStateFilter filter = AppStateFilter.valueOf(rawParamName.toUpperCase());
        return Pair.with(filter, paramVal);
      } catch(NullPointerException | IllegalArgumentException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "param " + param.substring(0, aux) + " not supported - supported params:"
            + EnumSet.allOf(AppStateFilter.class),
          "exception extracting FilterBy param", e);
      }
    } else {
      AppStateFilter filter = AppStateFilter.valueOf(param.toUpperCase());
      return Pair.with(filter, true);
    }
  }
  
  public static Pair<String, String> extractXAttrParam(String param) throws GenericException {
    String[] xattrParts = param.split(":");
    if(xattrParts.length != 2 || xattrParts[0].isEmpty()) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.INFO,
        "malformed xattr:" + param);
    }
    return processXAttr(xattrParts[0], xattrParts[1]);
  }
  
  public static Pair<String, String> processXAttr(String key, String val) {
    String[] keyParts =key.split("\\.");
    StringJoiner keyj = new StringJoiner(".");
    if(keyParts.length == 1) {
      keyj.add(keyParts[0]).add("raw");
    } else {
      keyj.add(keyParts[0]).add("value");
      for(int i = 1; i < keyParts.length; i++) keyj.add(keyParts[i]);
    }
    return Pair.with(keyj.toString(), val);
  }
  
  public static QueryBuilder getQB(ElasticFilters filter, Object paramVal) throws GenericException {
    switch(filter.filterType()) {
      case EXACT:
        return termQuery(filter.elasticField(), paramVal);
      case LIKE:
        if (paramVal instanceof String) {
          String sVal = (String) paramVal;
          return fullTextSearch(filter.elasticField(), sVal);
        } else {
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
            "like queries only work on string values");
        }
      case RANGE_LT:
        return rangeQuery(filter.elasticField()).to(paramVal);
      case RANGE_GT:
        return rangeQuery(filter.elasticField()).from(paramVal);
      default:
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "unmanaged filter by filterType: " + filter.filterType());
    }
  }
  
  public static QueryBuilder getXAttrQB(String xattrAdjustedKey, String xattrVal) {
    return termQuery(xattrAdjustedKey, xattrVal.toLowerCase());
  }
  
  public static QueryBuilder getLikeXAttrQB(String xattrAdjustedKey, String xattrVal) {
    return fullTextSearch(xattrAdjustedKey, xattrVal);
  }
  
  public static QueryBuilder fullTextSearch(String key, String term) {
    return boolQuery()
      .should(matchPhraseQuery(key, term.toLowerCase()))
      .should(prefixQuery(key, term.toLowerCase()))
      .should(fuzzyQuery(key, term.toLowerCase()))
      .should(wildcardQuery(key, String.format("*%s*", term.toLowerCase())));
  }
  
  public static class Common {
    public static final String INODE_ID_FIELD = "inode_id";
    public static final String APP_ID_FIELD = "app_id";
    public static final String USER_ID_FIELD = "user_id";
    public static final String PROJECT_INODE_ID_FIELD = "project_i_id";
    public static final String DATASET_INODE_ID_FIELD = "dataset_i_id";
    public static final String PARENT_INODE_ID_FIELD = "parent_i_id";
    public static final String INODE_NAME_FIELD = "inode_name";
    public static final String PROJECT_NAME_FIELD = "project_name";
    public static final String ENTRY_TYPE_FIELD = "entry_type";
    public static final String PARTITION_ID = "partition_id";
  }
  
  public static class State {
    public static final String CREATE_TIMESTAMP_FIELD = "create_timestamp";
    public static final String READABLE_CREATE_TIMESTAMP_FIELD = "r_create_timestamp";
  }
  
  public static class ML {
    public static final String ML_ID = "ml_id";
    public static final String ML_TYPE = "ml_type";
  }
  
  public static class Op {
    public static final String INODE_OPERATION_FIELD = "inode_operation";
    public static final String LOGICAL_TIME_FIELD = "logical_time";
    public static final String TIMESTAMP_FIELD = "timestamp";
    public static final String READABLE_TIMESTAMP_FIELD = "r_timestamp";
  }
  
  public static class OpOptional {
    public static final String INODE_PATH = "inode_path";
    public static final String XATTR_NAME_FIELD = "xattr";
  }
}
