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
    PROJECT_I_ID("PROJECT_I_ID", "project_i_id", FilterType.EXACT, new LongFilterVal()),
    FILE_INODE_ID("FILE_INODE_ID", "inode_id", FilterType.EXACT, new LongFilterVal()),
    FILE_NAME("FILE_NAME", "i_name", FilterType.EXACT, new StringFilterVal()),
    FILE_NAME_LIKE("FILE_NAME_LIKE", "i_name", FilterType.LIKE, new StringFilterVal()),
    FILE_OPERATION("FILE_OPERATION", "inode_operation", FilterType.EXACT,
      new FileOpFilterVal()),
    USER_ID("USER_ID", "io_user_id", FilterType.EXACT, new IntFilterVal()),
    APP_ID("APP_ID", "io_app_id", FilterType.EXACT, new StringFilterVal()),
    TIMESTAMP_LT("TIMESTAMP_LT", "io_timestamp", FilterType.RANGE_LT,
      new LongFilterVal()),
    TIMESTAMP_GT("TIMESTAMP_GT", "io_timestamp", FilterType.RANGE_GT,
      new LongFilterVal());
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
    PROJECT_I_ID("PROJECT_I_ID", "project_i_id", FilterType.EXACT),
    INODE_ID("INODE_ID", "inode_id", FilterType.EXACT),
    FILE_NAME("FILE_NAME", "inode_name", FilterType.EXACT),
    FILE_NAME_LIKE("FILE_NAME_LIKE", "inode_name", FilterType.LIKE),
    USER_ID("USER_ID", "user_id", FilterType.EXACT),
    APP_ID("APP_ID", "app_id", FilterType.EXACT),
    CREATE_TIMESTAMP_LT("CREATE_TIMESTAMP_LT", "timestamp", FilterType.RANGE_LT),
    CREATE_TIMESTAMP_GT("CREATE_TIMESTAMP_GT", "timestamp", FilterType.RANGE_GT),
    ML_TYPE("ML_TYP", "ml_type", FilterType.EXACT),
    ML_ID("ML_ID", "ml_id", FilterType.EXACT);
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
  
  public static Pair<ProvElastic.FileStateFilter, Object> extractFileStateParam(String param) throws GenericException {
    if (param.contains(":")) {
      int aux = param.indexOf(':');
      String rawParamName = param.substring(0, aux);
      String paramVal = param.substring(aux+1);
      try {
        ProvElastic.FileStateFilter filter = ProvElastic.FileStateFilter.valueOf(rawParamName.toUpperCase());
        return Pair.with(filter, paramVal);
      } catch(NullPointerException | IllegalArgumentException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "param " + param.substring(0, aux) + " not supported - supported params:"
            + EnumSet.allOf(FileStateFilter.class),
          "exception extracting FilterBy param", e);
      }
    } else {
      ProvElastic.FileStateFilter filter = ProvElastic.FileStateFilter.valueOf(param.toUpperCase());
      return Pair.with(filter, true);
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
    public static final String FILE_STATE_FIELD = "alive";
    public static final String PARTITION_ID = "partition_id";
    public static final String PARENT_INODE_ID = "parent_inode_id";
  }
  
  public static class Ops {
    public static final String INODE_PATH = "inode_path";
  }
}
