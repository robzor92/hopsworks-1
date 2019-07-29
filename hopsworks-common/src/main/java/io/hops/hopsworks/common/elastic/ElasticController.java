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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.hops.hopsworks.common.dao.dataset.Dataset;
import io.hops.hopsworks.common.dao.dataset.DatasetFacade;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.dao.project.ProjectFacade;
import io.hops.hopsworks.common.dataset.DatasetController;
import io.hops.hopsworks.common.provenance.AppProvenanceHit;
import io.hops.hopsworks.common.provenance.GeneralQueryParams;
import io.hops.hopsworks.common.provenance.MLAssetAppState;
import io.hops.hopsworks.common.provenance.ProvAppFootprintType;
import io.hops.hopsworks.common.provenance.ProvElastic;
import io.hops.hopsworks.common.provenance.ProvFileHit;
import io.hops.hopsworks.common.provenance.ProvFileOpHit;
import io.hops.hopsworks.common.provenance.ProvFileStateHit;
import io.hops.hopsworks.common.provenance.ProvFileAppDetailsQueryParams;
import io.hops.hopsworks.common.provenance.ProvFileDetailsQueryParams;
import io.hops.hopsworks.common.provenance.ProvFileQueryParams;
import io.hops.hopsworks.common.provenance.ProvFilesParamBuilder;
import io.hops.hopsworks.common.provenance.ProvMLAssetDetailsQueryParams;
import io.hops.hopsworks.common.provenance.ProvMLAssetQueryParams;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ProjectException;
import io.hops.hopsworks.restutils.RESTCodes;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.common.util.HopsUtils;
import io.hops.hopsworks.common.util.Ip;
import io.hops.hopsworks.common.util.Settings;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.elasticsearch.index.query.BoolQueryBuilder;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.fuzzyQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;
import org.elasticsearch.index.query.RangeQueryBuilder;

/**
 *
 * <p>
 */
@Stateless
public class ElasticController {

  @EJB
  private Settings settings;
  @EJB
  private ProjectFacade projectFacade;
  @EJB
  private DatasetFacade datasetFacade;
  @EJB
  private DatasetController datasetController;

  private static final Logger LOG = Logger.getLogger(ElasticController.class.getName());

  private Client elasticClient = null;

  @PostConstruct
  private void initClient() {
    try {
      getClient();
    } catch (ServiceException ex) {
      LOG.log(Level.SEVERE, null, ex);
  
    }
  }

  @PreDestroy
  private void closeClient(){
    shutdownClient();
  }

  public List<ElasticHit> globalSearch(String searchTerm) throws ServiceException {
    //some necessary client settings
    Client client = getClient();

    //check if the index are up and running
    if (!this.indexExists(client, Settings.META_INDEX)) {
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_INDEX_NOT_FOUND,
        Level.SEVERE, "index: " + Settings.META_INDEX);
    }

    LOG.log(Level.INFO, "Found elastic index, now executing the query.");

    //hit the indices - execute the queries
    SearchRequestBuilder srb = client.prepareSearch(Settings.META_INDEX);
    srb = srb.setTypes(Settings.META_DEFAULT_TYPE);
    srb = srb.setQuery(this.globalSearchQuery(searchTerm.toLowerCase()));
    srb = srb.highlighter(new HighlightBuilder().field("name"));
    LOG.log(Level.INFO, "Global search Elastic query is: {0}", srb);
    ActionFuture<SearchResponse> futureResponse = srb.execute();
    SearchResponse response = futureResponse.actionGet();

    if (response.status().getStatus() == 200) {
      //construct the response
      List<ElasticHit> elasticHits = new LinkedList<>();
      if (response.getHits().getHits().length > 0) {
        SearchHit[] hits = response.getHits().getHits();

        for (SearchHit hit : hits) {
          ElasticHit eHit = new ElasticHit(hit);
          eHit.setLocalDataset(true);
          int inode_id = Integer.parseInt(hit.getId());
          List<Dataset> dsl = datasetFacade.findByInodeId(inode_id);
          if (!dsl.isEmpty() && dsl.get(0).isPublicDs()) {
            Dataset ds = dsl.get(0);
            eHit.setPublicId(ds.getPublicDsId());
          }
          elasticHits.add(eHit);
        }
      }

      return elasticHits;
    } else {
      //something went wrong so throw an exception
      shutdownClient();
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING, "Elasticsearch " +
        "error code: " + response.status().getStatus());
    }
  }

  public String findExperiment(String index, String app_id) throws ServiceException {

    Client client = getClient();

    SearchResponse searchResponse = client.prepareSearch(index)
        .setQuery(QueryBuilders.matchQuery("app_id", app_id))
        .get();

    int status = searchResponse.status().getStatus();
    if(status != 200) {
      LOG.log(Level.SEVERE, "Unexpected response code " + searchResponse.status().getStatus() +
          " when updating experiment in Elastic. " + searchResponse.toString());
    }

    return searchResponse.toString();
  }

  public void updateExperiment(String index, String id, JSONObject source) throws IOException, ServiceException {

    Client client = getClient();

    Map<String, Object> map;

    ObjectMapper mapper = new ObjectMapper();
    map = mapper.readValue(source.toString(),
        new TypeReference<HashMap<String, Object>>() {
        });

    IndexResponse indexResponse = client.prepareIndex(index, "experiments", id)
        .setSource(map)
        .get();

    int status = indexResponse.status().getStatus();
    if(status != 200) {
      LOG.log(Level.SEVERE, "Unexpected response code " + indexResponse.status().getStatus() +
              " when updating experiment in Elastic. " + indexResponse.toString());
    }

  }

  public List<ElasticHit> projectSearch(Integer projectId, String searchTerm) throws ServiceException {
    Client client = getClient();
    //check if the index are up and running
    if (!this.indexExists(client, Settings.META_INDEX)) {
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_INDEX_NOT_FOUND,
        Level.SEVERE, "index: " + Settings.META_INDEX);
    } else if (!this.typeExists(client, Settings.META_INDEX,
        Settings.META_DEFAULT_TYPE)) {
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_INDEX_TYPE_NOT_FOUND, Level.SEVERE,
        "type: " + Settings.META_DEFAULT_TYPE);
    }

    SearchRequestBuilder srb = client.prepareSearch(Settings.META_INDEX);
    srb = srb.setTypes(Settings.META_DEFAULT_TYPE);
    srb = srb.setQuery(projectSearchQuery(projectId, searchTerm.toLowerCase()));
    srb = srb.highlighter(new HighlightBuilder().field("name"));

    LOG.log(Level.INFO, "Project Elastic query is: {0} {1}", new String[]{
      String.valueOf(projectId), srb.toString()});
    ActionFuture<SearchResponse> futureResponse = srb.execute();
    SearchResponse response = futureResponse.actionGet();

    if (response.status().getStatus() == 200) {
      //construct the response
      List<ElasticHit> elasticHits = new LinkedList<>();
      if (response.getHits().getHits().length > 0) {
        SearchHit[] hits = response.getHits().getHits();
        ElasticHit eHit;
        for (SearchHit hit : hits) {
          eHit = new ElasticHit(hit);
          eHit.setLocalDataset(true);
          elasticHits.add(eHit);
        }
      }

      projectSearchInSharedDatasets(client, projectId, searchTerm, elasticHits);
      return elasticHits;
    }

    shutdownClient();
    throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.SEVERE);
  }

  public List<ElasticHit> datasetSearch(Integer projectId, String datasetName, String searchTerm)
    throws ServiceException {
    Client client = getClient();
    //check if the indices are up and running
    if (!this.indexExists(client, Settings.META_INDEX)) {
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_INDEX_NOT_FOUND,
        Level.SEVERE, "index: " + Settings.META_INDEX);
    } else if (!this.typeExists(client, Settings.META_INDEX,
        Settings.META_DEFAULT_TYPE)) {
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_INDEX_TYPE_NOT_FOUND, Level.SEVERE,
        "type: " + Settings.META_DEFAULT_TYPE);
    }

    String dsName = datasetName;
    Project project;
    if (datasetName.contains(Settings.SHARED_FILE_SEPARATOR)) {
      String[] sharedDS = datasetName.split(Settings.SHARED_FILE_SEPARATOR);
      dsName = sharedDS[1];
      project = projectFacade.findByName(sharedDS[0]);
    } else {
      project = projectFacade.find(projectId);
    }

    Dataset dataset = datasetController.getByProjectAndDsName(project,null, dsName);
    final long datasetId = dataset.getInodeId();

    //hit the indices - execute the queries
    SearchRequestBuilder srb = client.prepareSearch(Settings.META_INDEX);
    srb = srb.setTypes(Settings.META_DEFAULT_TYPE);
    srb = srb.setQuery(this.datasetSearchQuery(datasetId, searchTerm.toLowerCase()));

    LOG.log(Level.INFO, "Dataset Elastic query is: {0}", srb.toString());
    ActionFuture<SearchResponse> futureResponse = srb.execute();
    SearchResponse response = futureResponse.actionGet();

    if (response.status().getStatus() == 200) {
      //construct the response
      List<ElasticHit> elasticHits = new LinkedList<>();
      if (response.getHits().getHits().length > 0) {
        SearchHit[] hits = response.getHits().getHits();
        ElasticHit eHit;
        for (SearchHit hit : hits) {
          eHit = new ElasticHit(hit);
          eHit.setLocalDataset(true);
          elasticHits.add(eHit);
        }
      }
      return elasticHits;
    }

    shutdownClient();
    throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.SEVERE);
  }

  public boolean deleteIndex(String index) throws ServiceException {
    boolean acked = getClient().admin().indices().delete(new DeleteIndexRequest(index)).actionGet().isAcknowledged();
    if (acked) {
      LOG.log(Level.INFO, "Acknowledged deletion of elastic index:{0}", index);
    } else {
      LOG.log(Level.SEVERE, "Elastic index:{0} deletion could not be acknowledged", index);
    }
    return acked;
  }

  public boolean indexExists(String index) throws ServiceException {

    boolean exists = getClient().admin().indices().exists(new IndicesExistsRequest(index)).actionGet().isExists();
    if (exists) {
      LOG.log(Level.FINE, "Elastic index found:{0}", index);
    } else {
      LOG.log(Level.FINE, "Elastic index:{0} could not be found", index);
    }
    return exists;
  }

  public void createIndex(String index) throws ServiceException {

    boolean acked = getClient().admin().indices().create(new CreateIndexRequest(index)).actionGet().isAcknowledged();
    if (!acked) {
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_INDEX_CREATION_ERROR,  Level.SEVERE,
        "Elastic index:{0} creation could not be acknowledged. index: " + index);
    }
  }

  public void createIndexPattern(Project project, String pattern) throws ProjectException {
    Map<String, String> params = new HashMap<>();
    params.put("op", "POST");
    params.put("data", "{\"attributes\": {\"title\": \"" + pattern + "\"}}");

    JSONObject resp = sendKibanaReq(params, "index-pattern", pattern);

    if (!(resp.has("updated_at") || (resp.has("statusCode") && resp.get("statusCode").toString().equals("409")))) {
      throw new ProjectException(RESTCodes.ProjectErrorCode.PROJECT_KIBANA_CREATE_INDEX_ERROR, Level.SEVERE, null,
        "project: " + project.getName() + ", resp: " + resp.toString(2), null);
    }

  }

  public void deleteProjectIndices(Project project) throws ServiceException {
    //Get all project indices
    Map<String, IndexMetaData> indices = getIndices(project.getName() +
        "_(((logs|serving)-\\d{4}.\\d{2}.\\d{2})|("+ Settings.ELASTIC_EXPERIMENTS_INDEX + ")"
        + "| (" + Settings.ELASTIC_KAGENT_INDEX_PATTERN + "))");
    for (String index : indices.keySet()) {
      if (!deleteIndex(index)) {
        LOG.log(Level.SEVERE, "Could not delete project index:{0}", index);
      }
    }
  }

  /**
   * Deletes visualizations, saved searches and dashboards for a project.
   *
   * @param projects
   */
  public void deleteProjectSavedObjects(List<String> projects) {
    //Loop through all objects and

    Map<String, String> params = new HashMap<>();
    params.put("op", "GET");
    JSONArray allObjects = sendKibanaReq(params).getJSONArray("saved_objects");
    Map<String, String> objectsToDelete= new HashMap<>();

    for(int i = 0; i< allObjects.length(); i++){
      String index = getIndexFromKibana(allObjects.getJSONObject(i));
      LOG.log(Level.FINE, "deleteProjectSavedObjects-index:{0}", index);
      if (!Strings.isNullOrEmpty(index) && index.contains("_logs-*")) {
        String projectName = index.split("_logs-*")[0];
        if (projects.contains(projectName)) {
          objectsToDelete.
              put(allObjects.getJSONObject(i).getString("id"), allObjects.getJSONObject(i).getString("type"));
        }
      }
    }
    params.put("op", "DELETE");
    for (String id : objectsToDelete.keySet()) {
      LOG.log(Level.FINE, "deleteProjectSavedObjects-deleting id:{0}, of type:{1}", new Object[]{id,
        objectsToDelete.get(id)});
      sendKibanaReq(params, objectsToDelete.get(id), id);
    }

  }

  public Result deleteDocument(String index, String type, String id) throws ServiceException {
    return getClient().prepareDelete(index, type, id).get().getResult();
  }

  public Map<String,IndexMetaData> getIndices() throws ServiceException {
    return getIndices(null);
  }

  /**
   * Get all indices. If pattern parameter is provided, only indices matching the pattern will be returned.
   * @param regex
   * @return
   */
  public Map<String, IndexMetaData> getIndices(String regex) throws ServiceException {
    ImmutableOpenMap<String, IndexMetaData> indices = getClient().admin().cluster().prepareState().get().getState()
        .getMetaData().getIndices();

    Map<String, IndexMetaData> indicesMap = null;

    if (indices != null && !indices.isEmpty()) {
      indicesMap = new HashMap<>();
      Pattern pattern = null;
      if (regex != null) {
        pattern = Pattern.compile(regex);
      }
      for (Iterator<String> iter = indices.keysIt(); iter.hasNext();) {
        String index = iter.next();
        if (pattern == null || pattern.matcher(index).matches()) {
          indicesMap.put(index, indices.get(index));
        }
      }
    }
    return indicesMap;
  }

  private Client getClient() throws ServiceException {
    if (elasticClient == null) {
      final org.elasticsearch.common.settings.Settings settings
          = org.elasticsearch.common.settings.Settings.builder()
              .put("client.transport.sniff", true) //being able to retrieve other nodes
              .put("cluster.name", "hops").build();

      elasticClient = new PreBuiltTransportClient(settings)
          .addTransportAddress(new TransportAddress(
              new InetSocketAddress(getElasticIpAsString(),
                  this.settings.getElasticPort())));
    }
    return elasticClient;
  }

  private void projectSearchInSharedDatasets(Client client, Integer projectId,
      String searchTerm, List<ElasticHit> elasticHits) {
    Project project = projectFacade.find(projectId);
    Collection<Dataset> datasets = project.getDatasetCollection();
    for (Dataset ds : datasets) {
      if (ds.isShared()) {
        List<Dataset> dss = datasetFacade.findByInode(ds.getInode());
        for (Dataset sh : dss) {
          if (!sh.isShared()) {
            long datasetId = ds.getInodeId();
            executeProjectSearchQuery(client, searchSpecificDataset(datasetId,
                searchTerm), elasticHits);

            executeProjectSearchQuery(client, datasetSearchQuery(datasetId,
                searchTerm), elasticHits);

          }
        }
      }
    }
  }

  private void executeProjectSearchQuery(Client client, QueryBuilder query, List<ElasticHit> elasticHits) {
    SearchRequestBuilder srb = client.prepareSearch(Settings.META_INDEX);
    srb = srb.setTypes(Settings.META_DEFAULT_TYPE);
    srb = srb.setQuery(query);
    srb = srb.highlighter(new HighlightBuilder().field("name"));

    LOG.log(Level.INFO, "Project Elastic query in Shared Dataset : {0}", srb.toString());
    ActionFuture<SearchResponse> futureResponse = srb.execute();
    SearchResponse response = futureResponse.actionGet();

    if (response.status().getStatus() == 200) {
      if (response.getHits().getHits().length > 0) {
        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit hit : hits) {
          elasticHits.add(new ElasticHit(hit));
        }
      }
    }
  }

  private QueryBuilder searchSpecificDataset(Long datasetId, String searchTerm) {
    QueryBuilder dataset = matchQuery(Settings.META_ID, datasetId);
    QueryBuilder nameDescQuery = getNameDescriptionMetadataQuery(searchTerm);
    return boolQuery()
        .must(dataset)
        .must(nameDescQuery);
  }

  /**
   * Global search on datasets and projects.
   * <p/>
   * @param searchTerm
   * @return
   */
  private QueryBuilder globalSearchQuery(String searchTerm) {
    QueryBuilder nameDescQuery = getNameDescriptionMetadataQuery(searchTerm);
    QueryBuilder onlyDatasetsAndProjectsQuery = termsQuery(Settings.META_DOC_TYPE_FIELD,
        Settings.DOC_TYPE_DATASET, Settings.DOC_TYPE_PROJECT);
    QueryBuilder query = boolQuery()
        .must(onlyDatasetsAndProjectsQuery)
        .must(nameDescQuery);

    return query;
  }

  /**
   * Project specific search.
   * <p/>
   * @param searchTerm
   * @return
   */
  private QueryBuilder projectSearchQuery(Integer projectId, String searchTerm) {
    QueryBuilder projectIdQuery = termQuery(Settings.META_PROJECT_ID_FIELD, projectId);
    QueryBuilder nameDescQuery = getNameDescriptionMetadataQuery(searchTerm);
    QueryBuilder onlyDatasetsAndInodes = termsQuery(Settings.META_DOC_TYPE_FIELD,
        Settings.DOC_TYPE_DATASET, Settings.DOC_TYPE_INODE);

    QueryBuilder query = boolQuery()
        .must(projectIdQuery)
        .must(onlyDatasetsAndInodes)
        .must(nameDescQuery);

    return query;
  }

  /**
   * Dataset specific search.
   * <p/>
   * @param searchTerm
   * @return
   */
  private QueryBuilder datasetSearchQuery(long datasetId, String searchTerm) {
    QueryBuilder datasetIdQuery = termQuery(Settings.META_DATASET_ID_FIELD, datasetId);
    QueryBuilder query = getNameDescriptionMetadataQuery(searchTerm);
    QueryBuilder onlyInodes = termQuery(Settings.META_DOC_TYPE_FIELD,
        Settings.DOC_TYPE_INODE);

    QueryBuilder cq = boolQuery()
        .must(datasetIdQuery)
        .must(onlyInodes)
        .must(query);
    return cq;
  }

  /**
   * Creates the main query condition. Applies filters on the texts describing a
   * document i.e. on the description
   * <p/>
   * @param searchTerm
   * @return
   */
  private QueryBuilder getNameDescriptionMetadataQuery(String searchTerm) {

    QueryBuilder nameQuery = getNameQuery(searchTerm);
    QueryBuilder descriptionQuery = getDescriptionQuery(searchTerm);
    QueryBuilder metadataQuery = getMetadataQuery(searchTerm);

    QueryBuilder textCondition = boolQuery()
        .should(nameQuery)
        .should(descriptionQuery)
        .should(metadataQuery);

    return textCondition;
  }

  /**
   * Creates the query that is applied on the name field.
   * <p/>
   * @param searchTerm
   * @return
   */
  private QueryBuilder getNameQuery(String searchTerm) {

    //prefix name match
    QueryBuilder namePrefixMatch = prefixQuery(Settings.META_NAME_FIELD,
        searchTerm);

    QueryBuilder namePhraseMatch = matchPhraseQuery(Settings.META_NAME_FIELD,
        searchTerm);

    QueryBuilder nameFuzzyQuery = fuzzyQuery(
        Settings.META_NAME_FIELD, searchTerm);

    QueryBuilder wildCardQuery = wildcardQuery(Settings.META_NAME_FIELD,
        String.format("*%s*", searchTerm));

    QueryBuilder nameQuery = boolQuery()
        .should(namePrefixMatch)
        .should(namePhraseMatch)
        .should(nameFuzzyQuery)
        .should(wildCardQuery);

    return nameQuery;
  }

  /**
   * Creates the query that is applied on the text fields of a document. Hits
   * the description fields
   * <p/>
   * @param searchTerm
   * @return
   */
  private QueryBuilder getDescriptionQuery(String searchTerm) {

    //do a prefix query on the description field in case the user starts writing
    //a full sentence
    QueryBuilder descriptionPrefixMatch = prefixQuery(
        Settings.META_DESCRIPTION_FIELD, searchTerm);

    //a phrase query to match the dataset description
    QueryBuilder descriptionMatch = termsQuery(
        Settings.META_DESCRIPTION_FIELD, searchTerm);

    //add a phrase match query to enable results to popup while typing phrases
    QueryBuilder descriptionPhraseMatch = matchPhraseQuery(
        Settings.META_DESCRIPTION_FIELD, searchTerm);

    //add a fuzzy search on description field
    QueryBuilder descriptionFuzzyQuery = fuzzyQuery(
        Settings.META_DESCRIPTION_FIELD, searchTerm);

    QueryBuilder wildCardQuery = wildcardQuery(Settings.META_DESCRIPTION_FIELD,
        String.format("*%s*", searchTerm));

    QueryBuilder descriptionQuery = boolQuery()
        .should(descriptionPrefixMatch)
        .should(descriptionMatch)
        .should(descriptionPhraseMatch)
        .should(descriptionFuzzyQuery)
        .should(wildCardQuery);

    return descriptionQuery;
  }

  /**
   * Creates the query that is applied on the text fields of a document. Hits
   * the xattr fields
   * <p/>
   * @param searchTerm
   * @return
   */
  private QueryBuilder getMetadataQuery(String searchTerm) {

    QueryBuilder metadataQuery = queryStringQuery(String.format("*%s*",
        searchTerm))
        .lenient(Boolean.TRUE)
        .field(Settings.META_DATA_FIELDS);
    QueryBuilder nestedQuery = nestedQuery(Settings.META_DATA_NESTED_FIELD,
        metadataQuery, ScoreMode.Avg);

    return nestedQuery;
  }

  /**
   * Checks if a given index exists in elastic
   * <p/>
   * @param client
   * @param indexName
   * @return
   */
  private boolean indexExists(Client client, String indexName) {
    AdminClient admin = client.admin();
    IndicesAdminClient indices = admin.indices();

    IndicesExistsRequestBuilder indicesExistsRequestBuilder = indices.
        prepareExists(indexName);

    IndicesExistsResponse response = indicesExistsRequestBuilder
        .execute()
        .actionGet();

    return response.isExists();
  }

  /**
   * Checks if a given data type exists. It is a given that the index exists
   * <p/>
   * @param client
   * @param typeName
   * @return
   */
  private boolean typeExists(Client client, String indexName, String typeName) {
    AdminClient admin = client.admin();
    IndicesAdminClient indices = admin.indices();

    ActionFuture<TypesExistsResponse> action = indices.typesExists(
        new TypesExistsRequest(
            new String[]{indexName}, typeName));

    TypesExistsResponse response = action.actionGet();

    return response.isExists();
  }

  /**
   * Shuts down the client and clears the cache
   * <p/>
   */
  private void shutdownClient() {
    if (elasticClient != null) {
      elasticClient.admin().indices().clearCache(new ClearIndicesCacheRequest(
          Settings.META_INDEX));
      elasticClient.close();
      elasticClient = null;
    }
  }

  /**
   * Boots up a previously closed index
   */
  private void bootIndices(Client client) {

    client.admin().indices().open(new OpenIndexRequest(
        Settings.META_INDEX));
  }

  private String getElasticIpAsString() throws ServiceException {
    String addr = settings.getElasticIp();

    // Validate the ip address pulled from the variables
    if (!Ip.validIp(addr)) {
      try {
        InetAddress.getByName(addr);
      } catch (UnknownHostException ex) {
        throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_AVAILABLE, Level.SEVERE, null,
          ex.getMessage(),
          ex);

      }
    }

    return addr;
  }

  private JSONObject sendKibanaReq(String templateUrl, Map<String, String> params, boolean async) {
    if (async) {
      ClientBuilder.newClient()
          .target(templateUrl)
          .request()
          .async()
          .method(params.get("op"));
      return null;
    } else {
      if (params.containsKey("data")) {
        return new JSONObject(ClientBuilder.newClient()
            .target(templateUrl)
            .request()
            .header("kbn-xsrf", "required")
            .header("Content-Type", "application/json")
            .method(params.get("op"), Entity.json(params.get("data"))).readEntity(String.class));
      } else {
        return new JSONObject(ClientBuilder.newClient()
            .target(templateUrl)
            .request()
            .header("kbn-xsrf", "required")
            .method(params.get("op")).readEntity(String.class));
      }
    }
  }

  public JSONObject sendKibanaReq(Map<String, String> params) {
    String templateUrl = settings.getKibanaUri() + "/api/saved_objects";
    LOG.log(Level.INFO, templateUrl);
    return sendKibanaReq(templateUrl, params, false);
  }

  public JSONObject sendKibanaReq(Map<String, String> params, String kibanaType) {
    String templateUrl = settings.getKibanaUri() + "/api/saved_objects/" + kibanaType;
    LOG.log(Level.INFO, templateUrl);
    return sendKibanaReq(templateUrl, params, false);
  }

  public JSONObject sendKibanaReq(Map<String, String> params, String kibanaType, String id) {
    String templateUrl = settings.getKibanaUri() + "/api/saved_objects/" + kibanaType + "/" + id;
    LOG.log(Level.INFO, templateUrl);
    return sendKibanaReq(templateUrl, params, false);
  }

  public JSONObject sendKibanaReq(Map<String, String> params, String kibanaType, String id, boolean overwrite) {
    String templateUrl;
    if(overwrite) {
      templateUrl = settings.getKibanaUri() + "/api/saved_objects/" + kibanaType + "/" + id + "?overwrite=true";
    } else {
      templateUrl = settings.getKibanaUri() + "/api/saved_objects/" + kibanaType + "/" + id;
    }
    LOG.log(Level.INFO, templateUrl);
    return sendKibanaReq(templateUrl, params, false);
  }

  public String getIndexFromKibana(JSONObject json){
    String index = null;

    if (json.has("type")) {
      switch (json.getString("type")) {
        case Settings.ELASTIC_INDEX_PATTERN:
          index = json.getString("id");
          break;
        case Settings.ELASTIC_SAVED_SEARCH:
          index = new JSONObject(json
            .getJSONObject("attributes")
            .getJSONObject("kibanaSavedObjectMeta")
            .getString("searchSourceJSON")).getString("index");
          break;
        case Settings.ELASTIC_VISUALIZATION:
          if (json.has("attributes")) {
            if (json.getJSONObject("attributes").has("savedSearchId")) {
              //We get the searchId first and then the index
              String searchId = json.getJSONObject("attributes").getString("savedSearchId");
              //Then get search object and call function again
              Map<String, String> params = new HashMap<>();
              params.put("op", "GET");
              JSONObject savedSearch = sendKibanaReq(params, Settings.ELASTIC_SAVED_SEARCH, searchId);
              LOG.log(Level.FINE, "visualization-parent:{0}", savedSearch);
              index = getIndexFromKibana(savedSearch);
            } else if (HopsUtils.jsonKeyExists(json, "kibanaSavedObjectMeta")) {
              JSONObject objectMetaJson = new JSONObject(json
                .getJSONObject("attributes")
                .getJSONObject("kibanaSavedObjectMeta")
                .getString("searchSourceJSON"));
              if(objectMetaJson.has("index")){
                index = objectMetaJson.getString("index");
              }
            }
          }
          break;
        case Settings.ELASTIC_DASHBOARD:
          if(HopsUtils.jsonKeyExists(json, "panelsJSON")){
            String id = (String) new JSONArray((String) json.getJSONObject("attributes").get("panelsJSON"))
                .getJSONObject(0).get("id");
            LOG.log(Level.FINE, "dashboard-id:{0}", id);
            String type = (String) new JSONArray((String) json.getJSONObject("attributes").get("panelsJSON"))
                .getJSONObject(0).get("type");
            LOG.log(Level.FINE, "dashboard-type:{0}", type);

            //Get index from visualization/"saved search"
            //Get and parse all objects
            Map<String, String> params = new HashMap<>();
            params.put("op", "GET");
            JSONObject parent = sendKibanaReq(params, type, id);
            LOG.log(Level.FINE, "dashboard-parent:{0}", parent.toString());
            index = getIndexFromKibana(parent);
          }
          break;
        default:
          break;
      }
    }
    LOG.log(Level.FINE, "getIndexFromKibana-index:{0}", index);
    return index;
  }

  public String getIndex(JSONObject json) {
    String objectId = null;
    if (json.getJSONObject("attributes").has("savedSearchId")) {
      LOG.log(Level.FINE, "savedSearchId-1:{0}", objectId);
      String searchId = json.getJSONObject("attributes").getString("savedSearchId");
      LOG.log(Level.FINE, "savedSearchId-2:{0}", searchId);
      //Find the index from the savedsearchId
      Map<String, String> params = new HashMap<>();
      params.put("op", "GET");
      JSONObject savedSearch = sendKibanaReq(params, Settings.ELASTIC_SAVED_SEARCH,searchId);
      objectId = new JSONObject(savedSearch
          .getJSONObject("attributes")
          .getJSONObject("kibanaSavedObjectMeta")
          .getString("searchSourceJSON")).getString("index");
    } else if (HopsUtils.jsonKeyExists(json, "kibanaSavedObjectMeta")
        && new JSONObject(json
            .getJSONObject("attributes")
            .getJSONObject("kibanaSavedObjectMeta")
            .getString("searchSourceJSON")).has("index")) {
      objectId = new JSONObject(json
          .getJSONObject("attributes")
          .getJSONObject("kibanaSavedObjectMeta")
          .getString("searchSourceJSON")).getString("index");
    } else if (json.getString("type").equals("dashboard")
        && HopsUtils.jsonKeyExists(json, "panelsJSON")) {
      //We need to get the index name from the visualization or saved search this dashboard is
      //created from
      String id = (String) new JSONArray((String) json.getJSONObject("attributes")
          .get("panelsJSON")).getJSONObject(0).get("id");
      LOG.log(Level.FINE, "dashboard-id:{0}", id);
      String type = (String) new JSONArray((String) json.getJSONObject("attributes")
          .get("panelsJSON")).getJSONObject(0).get("type");
      LOG.log(Level.FINE, "dashboard-type:{0}", type);

      //Get index from visualization/"saved search"
      //Get and parse all objects
      Map<String, String> params = new HashMap<>();
      params.put("op", "GET");
      JSONObject allObjects = sendKibanaReq(params, type);
      JSONArray allObjectsArray = allObjects.getJSONArray("saved_objects");
      for (int j = 0; j < allObjectsArray.length(); j++) {
        LOG.log(Level.FINE, "Checking id:{0}", allObjectsArray.getJSONObject(j).getString("id"));
        if (allObjectsArray.getJSONObject(j).getString("id").equals(id)) {

          if (allObjectsArray.getJSONObject(j).getJSONObject("attributes").has("savedSearchId")) {
            String searchId = allObjectsArray.getJSONObject(j)
                .getJSONObject("attributes")
                .getString("savedSearchId");
            //Find the index from the savedsearchId
            params.put("op", "GET");
            JSONObject savedSearch = sendKibanaReq(params, Settings.ELASTIC_SAVED_SEARCH, searchId);
            objectId = new JSONObject(savedSearch
                .getJSONObject("attributes")
                .getJSONObject("kibanaSavedObjectMeta")
                .getString("searchSourceJSON")).getString("index");
          } else if (HopsUtils.
              jsonKeyExists(allObjectsArray.getJSONObject(j), "kibanaSavedObjectMeta")
              && new JSONObject(allObjectsArray.getJSONObject(j)
                  .getJSONObject("attributes")
                  .getJSONObject("kibanaSavedObjectMeta")
                  .getString("searchSourceJSON")).has("index")) {
            JSONObject newjson = new JSONObject(allObjectsArray.getJSONObject(j)
                .getJSONObject("attributes")
                .getJSONObject("kibanaSavedObjectMeta")
                .getString("searchSourceJSON"));
            objectId = newjson.getString("index");

            LOG.log(Level.FINE, "objectId to remove:{0}", objectId);
            break;
          }
        }
      }
    }
    return objectId;
  }

  public String getLogdirFromElastic(Project project, String elasticId) throws ProjectException {
    Map<String, String> params = new HashMap<>();
    params.put("op", "GET");
    String projectName = project.getName().toLowerCase();

    String experimentsIndex = projectName + "_experiments";

    String templateUrl = "http://"+settings.getElasticRESTEndpoint() + "/" +
        experimentsIndex + "/experiments/" + elasticId;

    boolean foundEntry = false;
    JSONObject resp = null;
    try {
      resp = sendKibanaReq(templateUrl, params, false);
      foundEntry = (boolean) resp.get("found");
    } catch (Exception ex) {
      throw new ProjectException(RESTCodes.ProjectErrorCode.TENSORBOARD_ELASTIC_INDEX_NOT_FOUND, Level.SEVERE,
        "project:" + project.getName()+ ", index: " + elasticId, ex.getMessage(), ex);
    }

    if(!foundEntry) {
      throw new ProjectException(RESTCodes.ProjectErrorCode.TENSORBOARD_ELASTIC_INDEX_NOT_FOUND, Level.WARNING,
        "project:" + project.getName()+ ", index: " + elasticId);
    }

    JSONObject source = resp.getJSONObject("_source");
    return (String)source.get("logdir");
  }
  
  //PROVENANCE
  static final int DEFAULT_PROVENANCE_QUERY_SIZE = 500;
  public List<ProvFileStateHit> provFileState(ProvFilesParamBuilder params)
    throws GenericException, ServiceException, ProjectException {
    return provFileState(params.fileDetails(), params.mlAssetDetails(), params.appDetails(),
      new GeneralQueryParams(false));
  }
  
  public long provFileStateCount(ProvFilesParamBuilder params)
    throws GenericException, ServiceException, ProjectException {
    return provFileStateCount(params.fileDetails(), params.mlAssetDetails(), params.appDetails(),
      new GeneralQueryParams(true));
  }
  
  public List<ProvFileStateHit> provFileState(
    ProvFileQueryParams fileParams, ProvMLAssetQueryParams mlAssetParams, ProvFileAppDetailsQueryParams appDetails)
    throws ServiceException, ProjectException {
    return provFileStateQuery(provFileStateQB(fileParams, mlAssetParams), appDetails,
      DEFAULT_PROVENANCE_QUERY_SIZE);
  }

  public List<ProvFileStateHit> provFileState(
    ProvFileDetailsQueryParams fileDetails, ProvMLAssetDetailsQueryParams mlAssetParams,
    ProvFileAppDetailsQueryParams appDetails, GeneralQueryParams queryParams)
    throws ServiceException, GenericException, ProjectException {
    if(queryParams.count) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "count should not be used with this query");
    }
    return provFileStateQuery(provFileStateQB(fileDetails, mlAssetParams), appDetails,
      DEFAULT_PROVENANCE_QUERY_SIZE);
  }

  public long provFileStateCount(ProvFileDetailsQueryParams fileParams, ProvMLAssetDetailsQueryParams mlAssetParams,
    ProvFileAppDetailsQueryParams appDetails, GeneralQueryParams queryParams)
    throws ServiceException, GenericException, ProjectException {
    if(queryParams.count && appDetails.withAppState) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "count(no source) and withAppState(multi) cannot be used together");
    }
    return provFileCountQuery(provFileStateQB(fileParams, mlAssetParams));
  }
  
  public List<ProvFileHit> provAppFootprint(String appId, ProvAppFootprintType footprintType) throws ServiceException {
    String inodeOperations[];
    switch(footprintType) {
      case ALL:
        inodeOperations = new String[0];
        break;
      case INPUT:
        inodeOperations = new String[]{"CREATE", "ACCESS_DATA"};
        break;
      case OUTPUT:
        inodeOperations = new String[]{"CREATE", "MODIFY_DATA", "DELETE"};
        break;
      case OUTPUT_ADDED:
      case TMP:
      case REMOVED:
        inodeOperations = new String[]{"CREATE", "DELETE"};
        break;
      default:
        throw new IllegalArgumentException("footprint type:" + footprintType + " not managed");
    }
  
    List<ProvFileOpHit> fileOps = provFileOps(appId, inodeOperations);
    Map<Long, ProvFileHit> files = new HashMap<>();
    Set<Long> filesAccessed = new HashSet<>();
    Set<Long> filesCreated = new HashSet<>();
    Set<Long> filesModified = new HashSet<>();
    Set<Long> filesDeleted = new HashSet<>();
    for(ProvFileOpHit fileOp : fileOps) {
      files.put(fileOp.getInodeId(), new ProvFileHit(fileOp.getInodeId(), fileOp.getInodeName()));
      switch(fileOp.getInodeOperation()) {
        case "CREATE":
          filesCreated.add(fileOp.getInodeId());
          break;
        case "DELETE":
          filesDeleted.add(fileOp.getInodeId());
          break;
        case "ACCESS_DATA":
          filesAccessed.add(fileOp.getInodeId());
          break;
        case "MODIFY_DATA":
          filesModified.add(fileOp.getInodeId());
          break;
        default:
      }
    }
    //filter files based on footprintTypes
    switch(footprintType) {
      case ALL:
        //nothing - return all results
        break;
      case INPUT: {
        //files read - that existed before app (not created by app)
        Set<Long> aux = new HashSet<>(filesAccessed);
        aux.removeAll(filesCreated);
        files.keySet().retainAll(aux);
      } break;
      case OUTPUT: {
        //files created or modified, but not deleted
        Set<Long> aux = new HashSet<>(filesCreated);
        aux.addAll(filesModified);
        aux.removeAll(filesDeleted);
        files.keySet().retainAll(aux);
      } break;
      case OUTPUT_ADDED: {
        //files created but not deleted
        Set<Long> aux = new HashSet<>(filesCreated);
        aux.removeAll(filesDeleted);
        files.keySet().retainAll(aux);
      } break;
      case TMP: {
        //files created and deleted
        Set<Long> aux = new HashSet<>(filesCreated);
        aux.retainAll(filesDeleted);
        files.keySet().retainAll(aux);
      } break;
      case REMOVED: {
        //files not created and deleted
        Set<Long> aux = new HashSet<>(filesDeleted);
        aux.removeAll(filesCreated);
        files.keySet().retainAll(aux);
      } break;
      default:
        //continue;
    }
    return new LinkedList<>(files.values());
  }
  
  public List<ProvFileOpHit> provFileOps(Long inodeId) throws ServiceException {
    return provFileOpQuery(provFileOpQB(inodeId, null, null), DEFAULT_PROVENANCE_QUERY_SIZE);
  }
  
  public List<ProvFileOpHit> provFileOps(String appId) throws ServiceException {
    return provFileOps(appId, new String[0]);
  }
  
  private List<ProvFileOpHit> provFileOps(String appId, String[] inodeOperations) throws ServiceException {
    QueryBuilder inodeOperationsQB = provInodeOperationsQB(inodeOperations);
    List<ProvFileOpHit> queryResult
      = provFileOpQuery(provFileOpQB(null, appId, inodeOperationsQB), DEFAULT_PROVENANCE_QUERY_SIZE);
    Collections.sort(queryResult, ProvFileOpHit.timestampComparator);
    return queryResult;
  }
    
  private SearchResponse rawQuery(String index, String docType, QueryBuilder query, int querySize)
    throws ServiceException {
    //some necessary client settings
    Client client = getClient();

    //check if the index are up and running
    if (!this.indexExists(client, index)) {
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_INDEX_NOT_FOUND, Level.SEVERE, 
        "index: " + index);
    }

    //hit the indices - execute the queries
    SearchRequestBuilder srb = client.prepareSearch(index);
    srb = srb.setTypes(docType);
    srb = srb.setQuery(query);
    srb = srb.setSize(querySize);
    LOG.log(Level.INFO, "index:{0} Elastic query: {1}", new Object[]{index, srb});
    ActionFuture<SearchResponse> futureResponse = srb.execute();
    SearchResponse response = futureResponse.actionGet();

    if (response.status().getStatus() == 200) {
      return response;
    } else {
      //something went wrong so throw an exception
      shutdownClient();
      throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_FOUND, Level.WARNING, 
        "Elasticsearch error code: " + response.status().getStatus());
    }
  }

  private long provFileCountQuery(QueryBuilder query) throws ServiceException {
    long count = rawQuery(Settings.ELASTIC_INDEX_FILE_PROVENANCE,
      Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE, query, 0)
      .getHits().totalHits;
    LOG.log(Level.WARNING, "query hits: {0}", count);
    return count;
  }
  
  private  List<ProvFileOpHit> provFileOpQuery(QueryBuilder query, int querySize) throws ServiceException {
    List<ProvFileOpHit> result = new LinkedList<>();
    SearchResponse searchResult = rawQuery(Settings.ELASTIC_INDEX_FILE_PROVENANCE,
      Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE, query, querySize);
    SearchHit[] rawHits = searchResult.getHits().getHits();
    LOG.log(Level.WARNING, "query hits: {0} total:{1}",
      new Object[]{rawHits.length, searchResult.getHits().totalHits});
    for (SearchHit rawHit : rawHits) {
      ProvFileOpHit hit = new ProvFileOpHit(rawHit);
      result.add(hit);
    }
    return result;
  }
  
  private List<ProvFileStateHit> provFileStateQuery(QueryBuilder query,
    ProvFileAppDetailsQueryParams appDetails, int querySize)
    throws ServiceException {
    List<ProvFileStateHit> result = new LinkedList<>();
    Set<String> appIds = new HashSet<>();
    SearchHit[] rawHits = rawQuery(Settings.ELASTIC_INDEX_FILE_PROVENANCE,
      Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE, query, querySize)
      .getHits().getHits();
    LOG.log(Level.WARNING, "query hits: {0}", rawHits.length);
    for (SearchHit rawHit : rawHits) {
      ProvFileStateHit fpHit = new ProvFileStateHit(rawHit);
      result.add(fpHit);
      if(appDetails.withAppState) {
        appIds.add(getAppId(fpHit));
      }
    }
    if(appDetails.withAppState) {
      attachAppStates(result, appIds, appDetails.currentState, querySize);
    }
    return result;
  }
  
  private void attachAppStates(List<ProvFileStateHit> fileStates, Set<String> appIds,
    Provenance.AppState filterOnCurrentState, int querySize) throws ServiceException {
    Map<String, Map<Provenance.AppState, AppProvenanceHit>> applicationsStates = appStates(appIds, querySize);
    Iterator<ProvFileStateHit> it = fileStates.iterator();
    while(it.hasNext()) {
      ProvFileStateHit mlAsset = it.next();
      Map<Provenance.AppState, AppProvenanceHit> appStates = applicationsStates.get(getAppId(mlAsset));
      if(filterOnCurrentState != null) {
        if(appStates == null || !appStates.containsKey(filterOnCurrentState)) {
          it.remove();
        } else {
          mlAsset.setAppState(buildAppState(appStates));
        }
      } else {
        if (appStates != null) {
          mlAsset.setAppState(buildAppState(appStates));
        }
      }
    }
  }
  
  private MLAssetAppState buildAppState(Map<Provenance.AppState, AppProvenanceHit> appStates)
    throws ServiceException {
    MLAssetAppState mlAssetAppState = new MLAssetAppState();
    //app states is an ordered map
    //I assume values will still be ordered based on keys
    //if this is the case, the correct progression is SUBMITTED->RUNNING->FINISHED/KILLED/FAILED
    //as such just iterating over the states will provide us with the correct current state
    for (AppProvenanceHit appState : appStates.values()) {
      mlAssetAppState.setAppState(appState.getAppState(), appState.getAppStateTimestamp());
    }
    return mlAssetAppState;
  }
  
  private String getAppId(ProvFileStateHit mlAsset) {
    if(mlAsset.getAppId().equals("notls")) {
      if(mlAsset.getXattrs().containsKey("appId")) {
        return mlAsset.getXattrs().get("appId");
      } else {
        throw new IllegalArgumentException("withAppId enabled for tls clusters or notls cluster with xattr appIds");
      }
    } else {
      return mlAsset.getAppId();
    }
  }
  
  private Map<String, Map<Provenance.AppState, AppProvenanceHit>> appStates(Set<String> appIds, int querySize)
    throws ServiceException {
    SearchHit[]  rawHits = rawQuery(Settings.ELASTIC_INDEX_APP_PROVENANCE, 
      Settings.ELASTIC_INDEX_APP_PROVENANCE_DEFAULT_TYPE, appProvenanceByAppIdQuery(appIds), querySize)
      .getHits().getHits();
//    LOG.log(Level.WARNING, "query hits: {0}", rawHits.length);
    Map<String, Map<Provenance.AppState, AppProvenanceHit>> result = new HashMap<>();
    for(SearchHit h : rawHits) {
      AppProvenanceHit hit = new AppProvenanceHit(h);
      Map<Provenance.AppState, AppProvenanceHit> appStates = result.get(hit.getAppId());
      if(appStates == null) {
        appStates = new TreeMap<>();
        result.put(hit.getAppId(), appStates);
      }
      appStates.put(hit.getAppState(), hit);
    }
    return result;
  }

  private QueryBuilder getSearchTermQuery(String fieldName, String searchTerm) {
    QueryBuilder namePrefixMatch = prefixQuery(fieldName, searchTerm);
    QueryBuilder namePhraseMatch = matchPhraseQuery(fieldName, searchTerm);
    QueryBuilder nameFuzzyQuery = fuzzyQuery(fieldName, searchTerm);
    QueryBuilder wildCardQuery = wildcardQuery(fieldName, String.format("*%s*", searchTerm));

    QueryBuilder nameQuery = boolQuery()
      .should(namePrefixMatch)
      .should(namePhraseMatch)
      .should(nameFuzzyQuery)
      .should(wildCardQuery);

    return nameQuery;
  }
  
  private long getProjectInodeId(int projectId) throws ProjectException {
    Project project = projectFacade.find(projectId);
    if (project == null) {
      throw new ProjectException(RESTCodes.ProjectErrorCode.PROJECT_NOT_FOUND, Level.INFO,
        "projectId:" + projectId);
    }
    return project.getInode().getId();
  }
  
  private QueryBuilder provFileStateQB(ProvFileDetailsQueryParams fileDetails,
    ProvMLAssetDetailsQueryParams mlAssetListParams)
    throws ProjectException {
    BoolQueryBuilder query = boolQuery()
      .must(termQuery(ProvFileStateHit.ML_ALIVE_FIELD, true));
    if(fileDetails.projectId != null)
      query = query
        .must(termQuery(ProvFileStateHit.ML_PROJECT_INODE_ID_FIELD, getProjectInodeId(fileDetails.projectId)));
    if(fileDetails.assetName != null)
      query = query.must(matchQuery(ProvFileStateHit.ML_INODE_NAME_FIELD, fileDetails.assetName));
    if(fileDetails.likeAssetName != null)
      query = query.must(getSearchTermQuery(ProvFileStateHit.ML_INODE_NAME_FIELD, fileDetails.likeAssetName));
    if(fileDetails.userName != null)
      query = query.must(matchQuery(ProvFileStateHit.ML_USER_NAME_FIELD, fileDetails.userName));
    if(fileDetails.likeUserName != null)
      query = query.must(getSearchTermQuery(ProvFileStateHit.ML_USER_NAME_FIELD, fileDetails.likeUserName));
    if(fileDetails.createdBeforeTimestamp != null || fileDetails.createdAfterTimestamp != null) {
      RangeQueryBuilder rqb = rangeQuery(ProvFileStateHit.ML_CREATE_TIME_FIELD);
      if(fileDetails.createdAfterTimestamp != null) {
        rqb = rqb.from(fileDetails.createdAfterTimestamp);
      }
      if(fileDetails.createdBeforeTimestamp != null) {
        rqb = rqb.to(fileDetails.createdBeforeTimestamp);
      }
      query = query.must(rqb);
    }
    if(fileDetails.xattrsExact != null) {
      for (Map.Entry<String, String> xattr : fileDetails.xattrsExact.entrySet()) {
        query = query.must(matchQuery(xattr.getKey(), xattr.getValue()));
      }
    }
    if(fileDetails.xattrsLike != null) {
      for (Map.Entry<String, String> xattr : fileDetails.xattrsLike.entrySet()) {
        query = query.must(getSearchTermQuery(xattr.getKey(), xattr.getValue()));
      }
    }
    if(mlAssetListParams.mlType != null)
      query = query.must(termQuery(ProvFileStateHit.ML_TYPE_FIELD, mlAssetListParams.mlType));
    if(fileDetails.appId != null)
      query = query.must(termQuery(ProvFileStateHit.ML_APP_ID_FIELD, fileDetails.appId));
    return query;
  }

  private QueryBuilder provFileStateQB(ProvFileQueryParams fileParams, ProvMLAssetQueryParams mlAssetParams)
    throws ProjectException {
//    LOG.log(Level.INFO, "exact ml asset:{0}", params);
    BoolQueryBuilder query = boolQuery()
      .must(termQuery(ProvFileStateHit.ML_ALIVE_FIELD, true));
    if(fileParams.projectId != null)
      query = query
        .must(termQuery(ProvFileStateHit.ML_PROJECT_INODE_ID_FIELD, getProjectInodeId(fileParams.projectId)));
    if(fileParams.inodeId != null)
      query = query.must(termQuery(ProvFileStateHit.ML_INODE_ID_FIELD, fileParams.inodeId));
    if(mlAssetParams.mlId != null)
      query = query.must(termQuery(ProvFileStateHit.ML_ID_FIELD, mlAssetParams.mlId));
    if(mlAssetParams.mlType != null)
      query = query.must(termQuery(ProvFileStateHit.ML_TYPE_FIELD, mlAssetParams.mlType));
//    LOG.log(Level.INFO, "query:{0}", query.toString());
    return query;
  }
  
  private QueryBuilder provInodeOperationsQB(String... inodeOps) {
    if(inodeOps.length == 0) {
      return null;
    }
    BoolQueryBuilder query = boolQuery();
    for(String inodeOp : inodeOps) {
      query = query.should(termQuery(ProvElastic.Common.INODE_OPERATION_FIELD, inodeOp));
    }
    return query;
  }
  
  private QueryBuilder provFileOpQB(Long inodeId, String appId, QueryBuilder inodeOperationsQB) {
    BoolQueryBuilder query = boolQuery()
      .must(termQuery(ProvElastic.Common.ENTRY_TYPE_FIELD, "operation"));
    if(inodeId != null)
      query = query.must(termQuery(ProvElastic.Common.INODE_ID_FIELD, inodeId));
    if(appId != null)
      query  = query.must(termQuery(ProvElastic.Common.APP_ID_FIELD, appId));
    if(inodeOperationsQB != null)
      query = query.must(inodeOperationsQB);
    return query;
  }
  
  private QueryBuilder appProvenanceByAppIdQuery(Set<String> appIds) {
    QueryBuilder query = termsQuery(AppProvenanceHit.APP_ID_FIELD, appIds);
    return query;
  }
  //END_PROVENANCE
}


