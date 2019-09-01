/*
 * This file is part of Hopsworks
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.common.elastic;

import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.javatuples.Pair;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

public class ElasticClientHelper {
  private static final Logger LOG = Logger.getLogger(HopsworksElasticClient.class.getName());
  
  public static <S> Pair<S, Long> searchBasic(HopsworksElasticClient heClient, SearchRequest request,
    ElasticResultParser<S> resultParser)
    throws ServiceException, GenericException {
    SearchResponse response;
    try {
      response = searchBasicInt(heClient, request);
      resultParser.apply(response.getHits().getHits());
      return Pair.with(resultParser.get(), response.getHits().getTotalHits());
    } catch (ServiceException e) {
      heClient.processException(e);
      throw e;
    }
  }
  
  public static <S> S searchScrolling(HopsworksElasticClient heClient, SearchRequest request,
    ElasticResultParser<S> resultParser)
    throws ServiceException, GenericException {
    SearchResponse response;
    long leftover;
    try {
      response = searchBasicInt(heClient, request);
      if(response.getHits().getTotalHits() > HopsworksElasticClient.MAX_PAGE_SIZE) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
          "Elasticsearch query items size is too big: " + response.getHits().getTotalHits());
      }
      leftover = response.getHits().totalHits - response.getHits().getHits().length;
      resultParser.apply(response.getHits().getHits());
      
      while (leftover > 0) {
        SearchScrollRequest next = nextScrollPage(response.getScrollId());
        response = searchScrollingInt(heClient, next);
        leftover = leftover - response.getHits().getHits().length;
        resultParser.apply(response.getHits().getHits());
      }
      return resultParser.get();
    } catch (ServiceException e) {
      heClient.processException(e);
      throw e;
    }
  }
  
  public static Long searchCount(HopsworksElasticClient heClient, SearchRequest request) throws ServiceException {
    SearchResponse response;
    try {
      response = searchBasicInt(heClient, request);
      return response.getHits().getTotalHits();
    } catch (ServiceException e) {
      heClient.processException(e);
      throw e;
    }
  }
  
  private static SearchResponse searchBasicInt(HopsworksElasticClient heClient, SearchRequest request)
    throws ServiceException {
    SearchResponse response;
    try {
      response = heClient.search(request).get();
    } catch (Throwable e) {
      LOG.log(Level.WARNING, "error querying elastic", e);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        "error querying elastic", "error querying elastic", e);
    }
    if (response.status().getStatus() != 200) {
      LOG.log(Level.INFO, "searchBasic query - bad status response:{0}", response.status().getStatus());
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        "searchBasic query - bad status response:" + response.status().getStatus());
    }
    return response;
  }
  
  private static SearchResponse searchScrollingInt(HopsworksElasticClient heClient, SearchScrollRequest request)
    throws ServiceException {
    SearchResponse response;
    try {
      response = heClient.searchScroll(request).get();
    } catch (Throwable e) {
      LOG.log(Level.WARNING, "error querying elastic", e);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        "error querying elastic", "error querying elastic", e);
    }
    if (response.status().getStatus() != 200) {
      LOG.log(Level.INFO, "searchBasic query - bad status response:{0}", response.status().getStatus());
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        "searchBasic query - bad status response:" + response.status().getStatus());
    }
    return response;
  }
  
  private static SearchScrollRequest nextScrollPage(String scrollId) {
    SearchScrollRequest ssr = new SearchScrollRequest(scrollId);
    ssr.scroll(TimeValue.timeValueMinutes(1));
    return ssr;
  }
  
  public static class ElasticResultParser<S> {
    S accumulator;
    CheckedBiConsumer<SearchHit[], S, GenericException> base;
    
    public ElasticResultParser(S accumulator, CheckedBiConsumer<SearchHit[], S, GenericException> base) {
      this.base = base;
      this.accumulator = accumulator;
    }
    
    public void apply(SearchHit[] searchHits) throws GenericException {
      base.accept(searchHits, accumulator);
    }
    
    public S get() {
      return accumulator;
    }
  }
  
  public static QueryBuilder fullTextSearch(String key, String term) {
    return boolQuery()
      .should(matchPhraseQuery(key, term.toLowerCase()))
      .should(prefixQuery(key, term.toLowerCase()))
      .should(fuzzyQuery(key, term.toLowerCase()))
      .should(wildcardQuery(key, String.format("*%s*", term.toLowerCase())));
  }
}
