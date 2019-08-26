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
package io.hops.hopsworks.common.elastic;

import io.hops.hopsworks.common.provenance.AppProvenanceHit;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.common.provenance.util.CheckedFunction;
import io.hops.hopsworks.common.provenance.util.CheckedSupplier;
import io.hops.hopsworks.common.provenance.v2.ProvElastic;
import io.hops.hopsworks.common.provenance.v2.ProvQuery;
import io.hops.hopsworks.common.provenance.v2.xml.FileOp;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateDTO;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

@Stateless
public class ProvenanceElasticController {
  private static final Logger LOG = Logger.getLogger(ProvenanceElasticController.class.getName());
  @EJB
  private HopsworksElasticClient heClient;
  
  public FileStateDTO.PList provFileState(
    Map<String, List<Pair<ProvQuery.Field, Object>>> fileStateFilters,
    List<Pair<ProvQuery.Field, SortOrder>> fileStateSortBy,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters,
    List<Pair<String, SortOrder>> xattrSortBy,
    Integer offset, Integer limit)
    throws GenericException, ServiceException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      baseSearchRequest(
        Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE)
        .andThen(provFileStateQB(fileStateFilters, xAttrsFilters, likeXAttrsFilters))
        .andThen(withFileStateOrder(fileStateSortBy, xattrSortBy))
        .andThen(withPagination(offset, limit));
    SearchRequest request = srF.get();
    Pair<List<FileState>, Long> searchResult = ElasticClientHelper.searchBasic(heClient, request, fileStateParser());
    return new FileStateDTO.PList(searchResult.getValue0(), searchResult.getValue1());
  }
  
  public Long provFileStateCount(
    Map<String, List<Pair<ProvQuery.Field, Object>>> fileStateFilters,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters)
    throws GenericException, ServiceException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      countSearchRequest(
        Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE)
        .andThen(provFileStateQB(fileStateFilters, xAttrsFilters, likeXAttrsFilters));
    SearchRequest request = srF.get();
    Long searchResult = ElasticClientHelper.searchCount(heClient, request);
    return searchResult;
  }
  
  public List<FileOp> provFileOps(
    Map<String, List<Pair<ProvQuery.Field, Object>>> fileOpsFilters,
    List<Pair<ProvQuery.Field, SortOrder>> fileOpsSortBy,
    Integer offset, Integer limit)
    throws GenericException, ServiceException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      baseSearchRequest(
        Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE)
        .andThen(provFileOpsQB(fileOpsFilters))
        .andThen(withFileOpsOrder(fileOpsSortBy))
        .andThen(withPagination(offset, limit));
    SearchRequest request = srF.get();
    Pair<List<FileOp>, Long> searchResult = ElasticClientHelper.searchBasic(heClient, request, fileOpsParser());
    return searchResult.getValue0();
  }
  
  public List<FileOp> provFileOps(
    Map<String, List<Pair<ProvQuery.Field, Object>>> fileOpsFilters)
    throws GenericException, ServiceException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      scrollingSearchRequest(
        Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE)
        .andThen(provFileOpsQB(fileOpsFilters));
    SearchRequest request = srF.get();
    List<FileOp> searchResult = ElasticClientHelper.searchScrolling(heClient, request, fileOpsParser());
    return searchResult;
  }
  
  public Long provFileOpsCount(
    Map<String, List<Pair<ProvQuery.Field, Object>>> fileOpsFilters)
    throws ServiceException, GenericException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      countSearchRequest(
        Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE)
        .andThen(provFileOpsQB(fileOpsFilters));
    SearchRequest request = srF.get();
    Long searchResult = ElasticClientHelper.searchCount(heClient, request);
    return searchResult;
  }
  
  public Map<String, Map<Provenance.AppState, AppProvenanceHit>> provAppState(
    Map<String, List<Pair<ProvQuery.Field, Object>>> appStateFilters)
    throws GenericException, ServiceException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      scrollingSearchRequest(
        Settings.ELASTIC_INDEX_APP_PROVENANCE,
        Settings.ELASTIC_INDEX_APP_PROVENANCE_DEFAULT_TYPE)
        .andThen(provAppStateQB(appStateFilters));
    SearchRequest request = srF.get();
    Map<String, Map<Provenance.AppState, AppProvenanceHit>> searchResult
      = ElasticClientHelper.searchScrolling(heClient, request, appStateParser());
    return searchResult;
  }
  
  private CheckedSupplier<SearchRequest, GenericException> baseSearchRequest(String index, String docType) {
    return () -> {
      SearchRequest sr = new SearchRequest(index)
        .types(docType);
      sr.source().size(HopsworksElasticClient.DEFAULT_PAGE_SIZE);
      return sr;
    };
  }
  
  private CheckedSupplier<SearchRequest, GenericException> scrollingSearchRequest(String index, String docType) {
    return () -> {
      SearchRequest sr = new SearchRequest(index)
        .types(docType)
        .scroll(TimeValue.timeValueMinutes(1));
      sr.source().size(HopsworksElasticClient.DEFAULT_PAGE_SIZE);
      return sr;
    };
  }
  
  private CheckedSupplier<SearchRequest, GenericException> countSearchRequest(String index, String docType) {
    return () -> {
      SearchRequest sr = new SearchRequest(index)
        .types(docType);
      sr.source().size(0);
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> withPagination(
    Integer offset, Integer limit) {
    return (SearchRequest sr) -> {
      if(offset != null) {
        sr.source().from(offset);
      }
      if(limit != null) {
        sr.source().size(limit);
      }
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> withFileStateOrder(
    List<Pair<ProvQuery.Field, SortOrder>> fileStateSortBy, List<Pair<String, SortOrder>> xattrSortBy) {
    return (SearchRequest sr) -> {
      //      if(fileStateSortBy.isEmpty() && xattrSortBy.isEmpty()) {
      //        srb.addSort(SortBuilders.fieldSort("_doc").order(SortOrder.ASC));
      //      } else {
      for (Pair<ProvQuery.Field, SortOrder> sb : fileStateSortBy) {
        sr.source().sort(SortBuilders.fieldSort(sb.getValue0().elasticFieldName()).order(sb.getValue1()));
      }
      for (Pair<String, SortOrder> sb : xattrSortBy) {
        sr.source().sort(SortBuilders.fieldSort(sb.getValue0()).order(sb.getValue1()));
      }
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> withFileOpsOrder(
    List<Pair<ProvQuery.Field, SortOrder>> fileOpsSortBy) {
    return (SearchRequest sr) -> {
      for (Pair<ProvQuery.Field, SortOrder> sb : fileOpsSortBy) {
        sr.source().sort(SortBuilders.fieldSort(sb.getValue0().elasticFieldName()).order(sb.getValue1()));
      }
      return sr;
    };
  }
  
  private ElasticClientHelper.ElasticResultParser<List<FileState>> fileStateParser() {
    return new ElasticClientHelper.ElasticResultParser<>(new LinkedList<>(),
      (SearchHit[] hits,  List<FileState> acc) -> {
        for (SearchHit rawHit :hits) {
          FileState hit = FileState.instance(rawHit);
          acc.add(hit);
        }
      });
  }
  
  private ElasticClientHelper.ElasticResultParser<List<FileOp>> fileOpsParser() {
    return new ElasticClientHelper.ElasticResultParser<>(new LinkedList<>(),
      (SearchHit[] hits,  List<FileOp> acc) -> {
        for (SearchHit rawHit : hits) {
          FileOp hit = FileOp.instance(rawHit);
          acc.add(hit);
        }
      });
  }
  
  private ElasticClientHelper.ElasticResultParser<Map<String, Map<Provenance.AppState, AppProvenanceHit>>>
    appStateParser() {
    return new ElasticClientHelper.ElasticResultParser<>(new HashMap<>(),
      (SearchHit[] hits, Map<String, Map<Provenance.AppState, AppProvenanceHit>> acc) -> {
        for(SearchHit h : hits) {
          AppProvenanceHit hit = new AppProvenanceHit(h);
          Map<Provenance.AppState, AppProvenanceHit> appStates = acc.get(hit.getAppId());
          if(appStates == null) {
            appStates = new TreeMap<>();
            acc.put(hit.getAppId(), appStates);
          }
          appStates.put(hit.getAppState(), hit);
        }
      });
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> provFileStateQB(
    Map<String, List<Pair<ProvQuery.Field, Object>>> fileStateFilters,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvElastic.FileAux.ENTRY_TYPE.toString().toLowerCase(),
          ProvElastic.EntryType.STATE.toString().toLowerCase()));
      query = provFilterQB(query, fileStateFilters);
      for (Map.Entry<String, String> filter : xAttrsFilters.entrySet()) {
        query = query.must(getXAttrQB(filter.getKey(), filter.getValue()));
      }
      for (Map.Entry<String, String> filter : likeXAttrsFilters.entrySet()) {
        query = query.must(getLikeXAttrQB(filter.getKey(), filter.getValue()));
      }
      sr.source().query(query);
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> provFileOpsQB(
    Map<String, List<Pair<ProvQuery.Field, Object>>> fileOpsFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvElastic.FileAux.ENTRY_TYPE.toString().toLowerCase(),
          ProvElastic.EntryType.OPERATION.toString().toLowerCase()));
      query = provFilterQB(query,  fileOpsFilters);
      sr.source().query(query);
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> provAppStateQB(
    Map<String, List<Pair<ProvQuery.Field, Object>>> appStateFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery();
      query = provFilterQB(query, appStateFilters);
      sr.source().query(query);
      return sr;
    };
  }
  
  private BoolQueryBuilder provFilterQB(BoolQueryBuilder query,
    Map<String, List<Pair<ProvQuery.Field, Object>>> filters) throws GenericException {
    for (Map.Entry<String, List<Pair<ProvQuery.Field, Object>>> fieldFilters : filters.entrySet()) {
      if (fieldFilters.getValue().size() == 1) {
        Pair<ProvQuery.Field, Object> fieldFilter = fieldFilters.getValue().get(0);
        query = query.must(getQB(fieldFilter.getValue0(), fieldFilter.getValue1()));
      } else if (fieldFilters.getValue().size() > 1) {
        BoolQueryBuilder fieldQuery = boolQuery();
        query = query.must(fieldQuery);
        for (Pair<ProvQuery.Field, Object> fieldFilter : fieldFilters.getValue()) {
          fieldQuery = fieldQuery.should(getQB(fieldFilter.getValue0(), fieldFilter.getValue1()));
        }
      }
    }
    return query;
  }
  
  public QueryBuilder getQB(ProvQuery.Field filter, Object paramVal) throws GenericException {
    switch(filter.filterType()) {
      case EXACT:
        return termQuery(filter.elasticFieldName(), paramVal);
      case LIKE:
        if (paramVal instanceof String) {
          String sVal = (String) paramVal;
          return fullTextSearch(filter.elasticFieldName(), sVal);
        } else {
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
            "like queries only work on string values");
        }
      case RANGE_LT:
        return rangeQuery(filter.elasticFieldName()).to(paramVal);
      case RANGE_GT:
        return rangeQuery(filter.elasticFieldName()).from(paramVal);
      default:
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
          "unmanaged filter by filterType: " + filter.filterType());
    }
  }
  
  public QueryBuilder getXAttrQB(String xattrAdjustedKey, String xattrVal) {
    return termQuery(xattrAdjustedKey, xattrVal.toLowerCase());
  }
  
  public QueryBuilder getLikeXAttrQB(String xattrAdjustedKey, String xattrVal) {
    return fullTextSearch(xattrAdjustedKey, xattrVal);
  }
  
  public QueryBuilder fullTextSearch(String key, String term) {
    return boolQuery()
      .should(matchPhraseQuery(key, term.toLowerCase()))
      .should(prefixQuery(key, term.toLowerCase()))
      .should(fuzzyQuery(key, term.toLowerCase()))
      .should(wildcardQuery(key, String.format("*%s*", term.toLowerCase())));
  }
}
