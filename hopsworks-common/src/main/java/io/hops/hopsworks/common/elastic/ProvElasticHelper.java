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

import io.hops.hopsworks.common.provenance.util.CheckedFunction;
import io.hops.hopsworks.common.provenance.v2.ProvElasticFields;
import io.hops.hopsworks.common.provenance.v2.ProvFileOps;
import io.hops.hopsworks.common.provenance.v2.xml.FileOp;
import io.hops.hopsworks.common.provenance.v2.xml.FileOpDTO;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

public class ProvElasticHelper {
  private static final Logger LOG = Logger.getLogger(HopsworksElasticClient.class.getName());
  
  public static GetIndexResponse mngIndexGet(HopsworksElasticClient heClient, GetIndexRequest request)
    throws ServiceException {
    GetIndexResponse response;
    try {
      LOG.log(Level.INFO, "request:{0}", request.toString());
      response = heClient.mngIndexGet(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg = "elastic index:" + request.indices() + "error during index get";
      LOG.log(Level.INFO, msg, e);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING, msg, msg, e);
    } catch (ServiceException e) {
      String msg = "elastic index:" + request.indices() + "error during index get";
      LOG.log(Level.INFO, msg, e);
      throw e;
    }
    return response;
  }
  
  public static CreateIndexResponse mngIndexCreate(HopsworksElasticClient heClient, CreateIndexRequest request)
    throws ServiceException {
    if(request.index().length() > 255) {
      String msg = "elastic index name is too long:" + request.index();
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING, msg);
    }
    if(!request.index().equals(request.index().toLowerCase())) {
      String msg = "elastic index names can only contain lower case:" + request.index();
      LOG.log(Level.INFO, msg);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING, msg);
    }
    CreateIndexResponse response;
    try {
      LOG.log(Level.INFO, "request:{0}", request.toString());
      response = heClient.mngIndexCreate(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg = "elastic index:" + request.index() + "error during index create";
      LOG.log(Level.INFO, msg, e);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING, msg, msg, e);
    } catch (ServiceException e) {
      String msg = "elastic index:" + request.index() + "error during index create";
      LOG.log(Level.INFO, msg, e);
      throw e;
    }
    if(response.isAcknowledged()) {
      return response;
    } else {
      String msg = "elastic index:" + request.index() + "creation could not be acknowledged";
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_INDEX_CREATION_ERROR, Level.WARNING, msg);
    }
  }
  
  public static DeleteIndexResponse mngIndexDelete(HopsworksElasticClient heClient, DeleteIndexRequest request)
    throws ServiceException {
    DeleteIndexResponse response;
    try {
      LOG.log(Level.INFO, "request:{0}", request.toString());
      response = heClient.mngIndexDelete(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg = "elastic index:" + request.indices()[0] + "error during index delete";
      LOG.log(Level.INFO, msg, e);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING, msg, msg, e);
    } catch (ElasticsearchException e) {
      if(e.status() == RestStatus.NOT_FOUND) {
        //no retries maybe?
      }
      String msg = "elastic index:" + request.indices()[0] + "error during index delete";
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING, msg, msg, e);
    } catch (ServiceException e) {
      String msg = "elastic index:" + request.indices()[0] + "error during index delete";
      LOG.log(Level.INFO, msg, e);
      throw e;
    }
    if(response.isAcknowledged()) {
      return response;
    } else {
      String msg = "elastic index:" + request.indices()[0] + "deletion could not be acknowledged";
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING, msg);
    }
  }
  
  public static <S> S getFileDoc(HopsworksElasticClient heClient, GetRequest request,
    CheckedFunction<Map<String, Object>, S, GenericException> resultParser) throws ServiceException, GenericException {
    GetResponse response;
    try {
      LOG.log(Level.INFO, "request:{0}", request.toString());
      response = heClient.get(request).get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.INFO, "error during file doc get", e);
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        "error during file doc get", "error during file doc get", e);
    } catch (ServiceException e) {
      LOG.log(Level.INFO, "error during file doc get", e);
      throw e;
    }
    if(response.isExists()) {
      return resultParser.apply(response.getSource());
    } else {
      return null;
    }
  }
  
  public static void indexDoc(HopsworksElasticClient heClient, IndexRequest request) throws ServiceException {
    IndexResponse response;
  
    try {
      response = heClient.index(request).get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.INFO, "error during file doc index", e);
      ServiceException ex = new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        "error during file doc index", "error during file doc index", e);
      heClient.processException(ex);
      throw ex;
    } catch (ServiceException e) {
      LOG.log(Level.INFO, "error during file doc index", e);
      throw e;
    }
    if (response.status().getStatus() != 201) {
      LOG.log(Level.INFO, "doc index - bad status response:{0}", response.status().getStatus());
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        "doc index - bad status response:" + response.status().getStatus());
    }
  }
  
  public static void updateDoc(HopsworksElasticClient heClient, UpdateRequest request) throws ServiceException {
    UpdateResponse response;
  
    try {
      response = heClient.update(request).get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.INFO, "error during file doc update", e);
      ServiceException ex = new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        "error during file doc update", "error during file doc update", e);
      heClient.processException(ex);
      throw ex;
    } catch (ServiceException e) {
      LOG.log(Level.INFO, "error during file doc update", e);
      throw e;
    }
    if (response.status().getStatus() != 200) {
      LOG.log(Level.INFO, "doc update - bad status response:{0}", response.status().getStatus());
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        "doc update - bad status response:" + response.status().getStatus());
    }
  }
  
  public static <H> Pair<H, Long> searchBasic(HopsworksElasticClient heClient, SearchRequest request,
    ElasticBasicResultProcessor<H> resultProcessor)
    throws ServiceException, GenericException {
    SearchResponse response;
    try {
      LOG.log(Level.INFO, "request:{0}", request.toString());
      response = searchBasicInt(heClient, request);
      resultProcessor.apply(response.getHits().getHits());
      Pair<H, Long> result = Pair.with(resultProcessor.get(), response.getHits().getTotalHits());
      return result;
    } catch (ServiceException e) {
      throw e;
    }
  }
  
  public static <S> Pair<S, Long> searchScrollingWithBasicAction(HopsworksElasticClient heClient, SearchRequest request,
    ElasticBasicResultProcessor<S> resultProcessor)
    throws ServiceException, GenericException {
    SearchResponse response;
    long leftover;
    LOG.log(Level.INFO, "request:{0}", request.toString());
    response = searchBasicInt(heClient, request);
    if(response.getHits().getTotalHits() > HopsworksElasticClient.MAX_PAGE_SIZE) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
        "Elasticsearch query items size is too big: " + response.getHits().getTotalHits());
    }
    long totalHits = response.getHits().totalHits;
    leftover = totalHits - response.getHits().getHits().length;
    resultProcessor.apply(response.getHits().getHits());
    
    while (leftover > 0) {
      SearchScrollRequest next = nextScrollPage(response.getScrollId());
      response = searchScrollingInt(heClient, next);
      leftover = leftover - response.getHits().getHits().length;
      resultProcessor.apply(response.getHits().getHits());
    }
    return Pair.with(resultProcessor.get(), totalHits);
  }
  
  public static <S> S searchScrollingWithComplexAction(HopsworksElasticClient heClient, SearchRequest request,
    ElasticComplexResultProcessor<S> resultProcessor)
    throws ServiceException, GenericException {
    SearchResponse response;
    long leftover;
    LOG.log(Level.INFO, "request:{0}", request.toString());
    response = searchBasicInt(heClient, request);
    if(response.getHits().getTotalHits() > HopsworksElasticClient.MAX_PAGE_SIZE) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
        "Elasticsearch query items size is too big: " + response.getHits().getTotalHits());
    }
    leftover = response.getHits().totalHits - response.getHits().getHits().length;
    resultProcessor.apply(response.getHits().getHits());
    
    while (leftover > 0) {
      SearchScrollRequest next = nextScrollPage(response.getScrollId());
      LOG.log(Level.INFO, "request:{0}", next.toString());
      response = searchScrollingInt(heClient, next);
      leftover = leftover - response.getHits().getHits().length;
      resultProcessor.apply(response.getHits().getHits());
    }
    return resultProcessor.get();
  }
  
  public static Pair<Long, List<Pair<ProvElasticHelper.ProvAggregations, List>>> searchCount(
    HopsworksElasticClient heClient, SearchRequest request, List<ProvElasticHelper.ProvAggregations> aggregations)
    throws ServiceException, GenericException {
    SearchResponse response;
    LOG.log(Level.INFO, "request:{0}", request.toString());
    response = searchBasicInt(heClient, request);
    LOG.log(Level.INFO, "response:{0}", response.toString());
    if(aggregations.isEmpty()) {
      return Pair.with(response.getHits().getTotalHits(), Collections.emptyList());
    } else {
      List<Pair<ProvElasticHelper.ProvAggregations, List>> aggResults = new LinkedList<>();
      for (ProvElasticHelper.ProvAggregations aggregation : aggregations) {
        aggResults.add(Pair.with(aggregation, aggregation.parser.apply(response.getAggregations())));
      }
      return Pair.with(response.getHits().getTotalHits(), aggResults);
    }
  }
  
  public static void bulkDelete(HopsworksElasticClient heClient, BulkRequest request)
    throws ServiceException, GenericException {
    BulkResponse response;
    try {
      LOG.log(Level.INFO, "request:{0}", request.toString());
      response = heClient.bulkOp(request).get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.INFO, "error during bulk delete", e);
      ServiceException ex = new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        "error during bulk delete", "error during bulk delete", e);
      heClient.processException(ex);
      throw ex;
    } catch (ServiceException e) {
      LOG.log(Level.INFO, "service error during bulk delete", e);
      throw e;
    }
    if(response.hasFailures()) {
      LOG.log(Level.INFO, "failures during bulk delete");
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
        "failures during bulk delete");
    }
  }
  
  private static SearchResponse searchBasicInt(HopsworksElasticClient heClient, SearchRequest request)
    throws ServiceException {
    SearchResponse response;
    try {
      response = heClient.search(request).get();
    } catch (InterruptedException | ExecutionException e) {
      String msg = "error querying elastic index:" + request.indices()[0];
      LOG.log(Level.WARNING, msg, e);
      ServiceException ex = new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        msg, e.getMessage(), e);
      heClient.processException(ex);
      throw ex;
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
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.WARNING, "error querying elastic", e);
      ServiceException ex = new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING,
        "error querying elastic", "error querying elastic", e);
      heClient.processException(ex);
      throw ex;
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
  
  public static class ElasticPairException extends Exception {
    private ServiceException e1 = null;
    private GenericException e2 = null;
    
    private ElasticPairException(ServiceException e1, GenericException e2) {
      super();
      this.e1 = e1;
      this.e2 = e2;
    }
  
    public static ElasticPairException instanceService(ServiceException e1) {
      return new ElasticPairException(e1, null);
    }
  
    public  static ElasticPairException instanceGeneric(GenericException e2) {
      return new ElasticPairException(null, e2);
    }
    
    public void check() throws ServiceException, GenericException {
      if(e1 != null) {
        throw e1;
      }
      if(e2 != null) {
        throw e2;
      }
    }
  }
  
  public static class ElasticBasicResultProcessor<S> {
    S accumulator;
    CheckedBiConsumer<SearchHit[], S, GenericException> base;
    
    public ElasticBasicResultProcessor(S accumulator, CheckedBiConsumer<SearchHit[], S, GenericException> base) {
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
  
  public static class ElasticComplexResultProcessor<S> {
    S accumulator;
    CheckedBiConsumer<SearchHit[], S, ElasticPairException> base;
  
    public ElasticComplexResultProcessor(S accumulator, CheckedBiConsumer<SearchHit[], S, ElasticPairException> base) {
      this.base = base;
      this.accumulator = accumulator;
    }
  
    public void apply(SearchHit[] searchHits) throws GenericException, ServiceException {
      try {
        base.accept(searchHits, accumulator);
      } catch (ElasticPairException e) {
        //throw inner exception;
        e.check();
      }
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
  
  public enum FilterByScripts {
    ;
    public final Script script;
    
    FilterByScripts(Script script) {
      this.script = script;
    }
  }
  
  public enum ProvAggregations {
    FILES_IN(
      filesInABuilder("files_in", ProvElasticFields.FileBase.INODE_ID),
      filesInAParser("files_in")),
    FILES_LEAST_ACTIVE_BY_LAST_ACCESSED(
      leastActiveByLastAccessedABuilder(
        "files_least_active_by_last_accessed", ProvElasticFields.FileBase.INODE_ID,
        "lastOpTimestamp", ProvElasticFields.FileOpsBase.TIMESTAMP),
      leastActiveByLastAccessedAParser("files_least_active_by_last_accessed", "lastOpTimestamp")),
    PROJECTS_LEAST_ACTIVE_BY_LAST_ACCESSED(
      leastActiveByLastAccessedABuilder(
        "projects_least_active_by_last_accessed", ProvElasticFields.FileBase.PROJECT_I_ID,
        "lastOpTimestamp", ProvElasticFields.FileOpsBase.TIMESTAMP),
      leastActiveByLastAccessedAParser("projects_least_active_by_last_accessed", "lastOpTimestamp")),
    ARTIFACT_FOOTPRINT(appArtifactFootprintABuilder(), appArtifactFootprintAParser());
    
    public final AggregationBuilder aggregation;
    public final CheckedFunction<Aggregations, List, GenericException> parser;
    ProvAggregations(AggregationBuilder aggregation, CheckedFunction<Aggregations, List, GenericException> parser) {
      this.aggregation = aggregation;
      this.parser = parser;
    }
  }
  
  private static AggregationBuilder filesInABuilder(String agg, ProvElasticFields.Field aggField) {
    return AggregationBuilders.terms(agg)
      .field(aggField.toString())
      .size(HopsworksElasticClient.DEFAULT_PAGE_SIZE);
  }
  
  private static CheckedFunction<Aggregations, List, GenericException> filesInAParser(String agg1Name) {
    return (Aggregations aggregations) -> {
      List<FileOpDTO.FileAggregation> result = new LinkedList<>();
      Terms agg1 = aggregations.get(agg1Name);
      if (agg1 == null) {
        return null;
      }
      List<? extends Terms.Bucket> buckets = agg1.getBuckets();
      for(Terms.Bucket bucket : buckets) {
        Long inodeId = (Long)bucket.getKeyAsNumber();
        result.add(new FileOpDTO.FileAggregation(inodeId, bucket.getDocCount()));
      }
      return result;
    };
  }
  
  private static AggregationBuilder leastActiveByLastAccessedABuilder(String agg1, ProvElasticFields.Field agg1Field,
    String agg2, ProvElasticFields.Field agg2Field) {
    return AggregationBuilders.terms(agg1)
      .field(agg1Field.toString())
      .size(HopsworksElasticClient.DEFAULT_PAGE_SIZE)
      .order(InternalOrder.aggregation(agg2, true))
      .subAggregation(
        AggregationBuilders.max(agg2).field(agg2Field.toString()));
  }
  
  private static CheckedFunction<Aggregations, List, GenericException> leastActiveByLastAccessedAParser(
    String agg1Name, String agg2Name) {
    return (Aggregations aggregations) -> {
      List<FileOpDTO.FileAggregation> result = new LinkedList<>();
      Terms agg1 = aggregations.get(agg1Name);
      if(agg1 == null) {
        return result;
      }
      List<? extends Terms.Bucket> agg1Buckets = agg1.getBuckets();
      for(Terms.Bucket bucket : agg1Buckets) {
        Max agg2 = bucket.getAggregations().get(agg2Name);
        Long inodeId = (Long)bucket.getKeyAsNumber();
        Long lastAccessed = ((Number) agg2.getValue()).longValue();
        result.add(new FileOpDTO.FileAggregation(inodeId, bucket.getDocCount(), lastAccessed));
      }
      return result;
    };
  }
  
  private static CheckedFunction<Aggregations, List, GenericException> appArtifactFootprintAParser() {
    return (Aggregations aggregations) -> {
      List<FileOpDTO.Artifact> result = new LinkedList<>();
      Terms artifacts = aggregations.get("artifacts");
      if(artifacts == null) {
        return result;
      }
      for(Terms.Bucket artifactBucket : artifacts.getBuckets()) {
        FileOpDTO.Artifact artifact = new FileOpDTO.Artifact();
  
        Terms files = artifactBucket.getAggregations().get("files");
        if(files == null) {
          continue;
        }
        result.add(artifact);
        
        FileOpDTO.ArtifactBase base = null;
        for(Terms.Bucket fileBucket : artifacts.getBuckets()) {
          FileOpDTO.ArtifactFile file = new FileOpDTO.ArtifactFile();
          //create
          Filter createFilter = fileBucket.getAggregations().get("create");
          if(createFilter != null) {
            TopHits createOpHits = createFilter.getAggregations().get("create_op");
            if(createOpHits.getHits().getTotalHits() > 1) {
              throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.WARNING,
                "cannot have two create ops on the same inode");
            }
            FileOp createOp = FileOp.instance(createOpHits.getHits().getAt(0), false);
            file.addCreate(createOp);
            base = extractBaseIfNotExists(base, createOp);
          }
  
          //delete
          Filter deleteFilter = fileBucket.getAggregations().get("delete");
          if(deleteFilter != null) {
            TopHits deleteOpHits = deleteFilter.getAggregations().get("delete_op");
            if(deleteOpHits.getHits().getTotalHits() > 1) {
              throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.WARNING,
                "cannot have two delete ops on the same inode");
            }
            FileOp deleteOp = FileOp.instance(deleteOpHits.getHits().getAt(0), false);
            file.addDelete(deleteOp);
            base = extractBaseIfNotExists(base, deleteOp);
          }
  
          //read
          Filter readFilter = fileBucket.getAggregations().get("read");
          if(readFilter != null) {
            TopHits readOpHits = readFilter.getAggregations().get("first_read");
            FileOp readOp = FileOp.instance(readOpHits.getHits().getAt(0), false);
            file.addFirstRead(readOp, readOpHits.getHits().getTotalHits());
            base = extractBaseIfNotExists(base, readOp);
          }
  
          //append
          Filter appendFilter = fileBucket.getAggregations().get("append");
          if(appendFilter != null) {
            TopHits appendOpHits = appendFilter.getAggregations().get("first_append");
            FileOp appendOp = FileOp.instance(appendOpHits.getHits().getAt(0), false);
            file.addFirstAppend(appendOp, appendOpHits.getHits().getTotalHits());
            base = extractBaseIfNotExists(base, appendOp);
          }
          artifact.addComponent(file);
        }
        artifact.setBase(base);
      }
      return result;
    };
  }
  
  private static FileOpDTO.ArtifactBase extractBaseIfNotExists(FileOpDTO.ArtifactBase base, FileOp op)
    throws GenericException {
    if(base != null) {
      return base;
    }
    ProvElasticFields.MLType mlType = ProvElasticFields.parseMLType(op.getMlType());
    return new FileOpDTO.ArtifactBase(op.getProjectInodeId(), op.getDatasetInodeId(), op.getMlId(), mlType);
  }
  
  private static AggregationBuilder appArtifactFootprintABuilder() {
    return
      AggregationBuilders.terms("artifacts")
        .field(ProvElasticFields.FileOpsAux.ML_ID.toString())
        .subAggregation(
          AggregationBuilders.terms("files")
            .field(ProvElasticFields.FileBase.INODE_ID.toString())
            .subAggregation(AggregationBuilders
              .filter("create", termQuery(ProvElasticFields.FileOpsBase.INODE_OPERATION.toString(),
                  ProvFileOps.CREATE.toString()))
              .subAggregation(AggregationBuilders
                .topHits("create_op")
                .sort(ProvElasticFields.FileOpsBase.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1)))
            .subAggregation(AggregationBuilders
              .filter("delete", termQuery(ProvElasticFields.FileOpsBase.INODE_OPERATION.toString(),
                ProvFileOps.ACCESS_DATA.toString()))
              .subAggregation(AggregationBuilders
                .topHits("delete_op")
                .sort(ProvElasticFields.FileOpsBase.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1)))
            .subAggregation(AggregationBuilders
              .filter("read", termQuery(ProvElasticFields.FileOpsBase.INODE_OPERATION.toString(),
                  ProvFileOps.ACCESS_DATA.toString()))
              .subAggregation(AggregationBuilders
                .topHits("first_read")
                .sort(ProvElasticFields.FileOpsBase.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1)))
            .subAggregation(AggregationBuilders
              .filter("append", termQuery(ProvElasticFields.FileOpsBase.INODE_OPERATION.toString(),
                  ProvFileOps.ACCESS_DATA.toString()))
              .subAggregation(AggregationBuilders
                .topHits("first_append")
                .sort(ProvElasticFields.FileOpsBase.TIMESTAMP.toString(), SortOrder.ASC)
                .size(1))));
  }
}
