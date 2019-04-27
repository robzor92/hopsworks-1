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

import com.google.gson.Gson;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.common.provenance.AppProvenanceHit;
import io.hops.hopsworks.common.provenance.Provenance;
import io.hops.hopsworks.common.provenance.util.CheckedFunction;
import io.hops.hopsworks.common.provenance.util.CheckedSupplier;
import io.hops.hopsworks.common.provenance.v2.ProvElasticFields;
import io.hops.hopsworks.common.provenance.v2.ProvFileQuery;
import io.hops.hopsworks.common.provenance.v2.ProvParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.ArchiveDTO;
import io.hops.hopsworks.common.provenance.v2.xml.FileOp;
import io.hops.hopsworks.common.provenance.v2.xml.FileOpDTO;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateDTO;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.idsQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Stateless
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
public class ProvElasticController {
  private static final Logger LOG = Logger.getLogger(ProvElasticController.class.getName());
  @EJB
  private HopsworksElasticClient heClient;
  @EJB
  private DistributedFsService dfs;
  
  public FileStateDTO.PList provFileState(
    Map<String, ProvFileQuery.FilterVal> fileStateFilters,
    List<Pair<ProvFileQuery.Field, SortOrder>> fileStateSortBy,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters,
    List<Pair<String, SortOrder>> xattrSortBy,
    Integer offset, Integer limit)
    throws GenericException, ServiceException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      baseSearchRequest(
        Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE)
        .andThen(filterByStateParams(fileStateFilters, xAttrsFilters, likeXAttrsFilters))
        .andThen(withFileStateOrder(fileStateSortBy, xattrSortBy))
        .andThen(withPagination(offset, limit));
    SearchRequest request = srF.get();
    Pair<List<FileState>, Long> searchResult = ProvElasticHelper
      .searchBasic(heClient, request, fileStateParser());
    return new FileStateDTO.PList(searchResult.getValue0(), searchResult.getValue1());
  }
  
  public Long provFileStateCount(
    Map<String, ProvFileQuery.FilterVal> fileStateFilters,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters)
    throws GenericException, ServiceException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      countSearchRequest(
        Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE)
        .andThen(filterByStateParams(fileStateFilters, xAttrsFilters, likeXAttrsFilters));
    SearchRequest request = srF.get();
    Long searchResult = ProvElasticHelper.searchCount(heClient, request,Collections.emptyList()).getValue0();
    return searchResult;
  }
  
  public FileOpDTO.PList provFileOpsBase(
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters,
    List<Script> scriptFilter,
    List<Pair<ProvFileQuery.Field, SortOrder>> fileOpsSortBy,
    Integer offset, Integer limit)
    throws GenericException, ServiceException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      baseSearchRequest(
        Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE)
        .andThen(filterByOpsParams(fileOpsFilters, scriptFilter))
        .andThen(withFileOpsOrder(fileOpsSortBy))
        .andThen(withPagination(offset, limit));
    SearchRequest request = srF.get();
    Pair<List<FileOp>, Long> searchResult
      = ProvElasticHelper.searchBasic(heClient, request, fileOpsParser());
    return new FileOpDTO.PList(searchResult.getValue0(), searchResult.getValue1());
  }
  
  public FileOpDTO.PList provFileOpsScrolling(
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters,
    List<Script> filterScripts)
    throws GenericException, ServiceException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      scrollingSearchRequest(
        Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE,
        HopsworksElasticClient.DEFAULT_PAGE_SIZE)
        .andThen(filterByOpsParams(fileOpsFilters, filterScripts));
    SearchRequest request = srF.get();
    Pair<List<FileOp>, Long> searchResult
      = ProvElasticHelper.searchScrollingWithBasicAction(heClient, request, fileOpsParser());
    return new FileOpDTO.PList(searchResult.getValue0(), searchResult.getValue1());
  }
  
  public FileOpDTO.Count provFileOpsCount(
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters,
    List<Script> filterScripts,
    List<ProvElasticHelper.ProvAggregations> aggregations)
    throws ServiceException, GenericException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      countSearchRequest(
        Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE)
        .andThen(filterByOpsParams(fileOpsFilters, filterScripts))
        .andThen(withAggregations(aggregations));
    SearchRequest request = srF.get();
    Pair<Long, List<Pair<ProvElasticHelper.ProvAggregations, List>>> result
      = ProvElasticHelper.searchCount(heClient, request, aggregations);
    return FileOpDTO.Count.instance(result);
  }
  
  public Map<String, Map<Provenance.AppState, AppProvenanceHit>> provAppState(
    Map<String, ProvFileQuery.FilterVal> appStateFilters)
    throws GenericException, ServiceException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      scrollingSearchRequest(
        Settings.ELASTIC_INDEX_APP_PROVENANCE,
        Settings.ELASTIC_INDEX_APP_PROVENANCE_DEFAULT_TYPE,
        HopsworksElasticClient.DEFAULT_PAGE_SIZE)
        .andThen(provAppStateQB(appStateFilters));
    SearchRequest request = srF.get();
    Pair<Map<String, Map<Provenance.AppState, AppProvenanceHit>>, Long> searchResult
      = ProvElasticHelper.searchScrollingWithBasicAction(heClient, request, appStateParser());
    return searchResult.getValue0();
  }
  
  //*** Archival
  private static class Archival {
    Long counter = 0l;
    Store store;
    Optional<Long> baseDoc;
    
    public Archival(Store store, Optional<Long> baseArchiveDoc) {
      this.store = store;
      this.baseDoc = baseArchiveDoc;
    }
    
    void incBy(int val) {
      counter += val;
    }
  }
  
  private interface Store {
    void init() throws GenericException;
    void addOp(FileOp op) throws GenericException;
    Long save() throws GenericException;
    String getLocation();
  }
  
  public static class NoStore implements Store {
  
    @Override
    public void init() throws GenericException {
    }
  
    @Override
    public void addOp(FileOp op) {
    }
  
    @Override
    public Long save() throws GenericException {
      return 0l;
    }
  
    @Override
    public String getLocation() {
      return "no_store";
    }
  }
  public static class Hops implements Store {
    String projectPath;
    String relativePath = "Resources/provenance";
    String archiveFile = "archive";
    String statusFile = "status";
    Long line = 0l;
    DistributedFileSystemOps dfso;
    FileOpDTO.File file;
    
    public Hops(String projectPath, DistributedFileSystemOps dfso) {
      this.projectPath = projectPath;
      this.dfso = dfso;
    }
    
    @Override
    public void init() throws GenericException {
      String dirPath;
      if(projectPath.endsWith("/")){
        dirPath = projectPath + relativePath;
      } else {
        dirPath = projectPath + "/" + relativePath;
      }
      
      String filePath = dirPath + "/" + archiveFile;
      String statusPath = dirPath + "/" + statusFile;
      try {
        if(!dfso.exists(dirPath)) {
          dfso.mkdir(dirPath);
        } else if(!dfso.isDir(dirPath)) {
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.WARNING,
            "the provenance dir cannot be created - a file with its name already exists");
        }
        if(!dfso.exists(filePath)) {
          try(FSDataOutputStream out = dfso.create(filePath)){
          }
        }
        if(!dfso.exists(statusPath)) {
          try(FSDataOutputStream out = dfso.create(statusPath)){
            out.writeLong(line);
            out.flush();
          }
        } else {
          try(FSDataInputStream in = dfso.open(statusPath)){
            line = in.readLong();
          }
        }
      } catch (IOException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.WARNING,
          "error while doing file on the fs - hops");
      }
    }
    
    @Override
    public void addOp(FileOp op) throws GenericException {
      if(file == null) {
        file = new FileOpDTO.File(op);
      } else {
        file.addOp(op);
      }
    }
    
    @Override
    public Long save() throws GenericException {
      String dirPath = projectPath + "/" + relativePath;
      String filePath = dirPath + "/" + archiveFile;
      String statusPath = dirPath + "/" + statusFile;
      try(FSDataOutputStream archive = dfso.append(filePath)) {
        if(!dfso.exists(projectPath)) {
          LOG.log(Level.INFO, "project:{0} does not exist anymore - can't use for archive", projectPath);
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.WARNING,
            "archival project does not exist anymore");
        }
        Gson gson = new Gson();
        String data = gson.toJson(file) + "\n";
        archive.writeBytes(data);
        archive.flush();
        line++;
        dfso.rm(new Path(statusPath), false);
        try(FSDataOutputStream status = dfso.create(statusPath)){
          status.writeLong(line);
          status.flush();
        }
        file = null;
        return line;
      } catch(IOException e) {
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.WARNING,
          "error while doing file on the fs - hops", "hops exception", e);
      }
    }
    
    @Override
    public String getLocation() {
      String dirPath = projectPath + "/" + relativePath;
      String filePath = dirPath + "/" + archiveFile;
      return filePath;
    }
    
  }
  
  private CheckedBiConsumer<SearchHit[], Archival, ProvElasticHelper.ElasticPairException> archivalConsumer
    = (SearchHit[] hits, Archival acc) -> {
      if(hits.length > 0) {
        try {
          for(SearchHit hit : hits) {
            acc.store.addOp(FileOp.instance(hit));
          }
          Long line = acc.store.save();
          if(acc.baseDoc.isPresent()) {
            updateArchive(acc.baseDoc.get(), acc.store.getLocation(), line);
          }
          
          BulkRequest bulkDelete = new BulkRequest();
          for (SearchHit hit : hits) {
            bulkDelete.add(new DeleteRequest(Settings.ELASTIC_INDEX_FILE_PROVENANCE,
              Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE, hit.getId()));
          }
          ProvElasticHelper.bulkDelete(heClient, bulkDelete);
          acc.incBy(hits.length);
        } catch (GenericException e) {
          throw ProvElasticHelper.ElasticPairException.instanceGeneric(e);
        } catch (ServiceException e) {
          throw ProvElasticHelper.ElasticPairException.instanceService(e);
        }
      }
    };
  
  public Long provArchiveFilePrefix(Long inodeId, Optional<Long> timestamp, String withArchiveProject)
    throws GenericException, ServiceException {
    createArchive(inodeId);
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters = new HashMap<>();
    ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOps.FILE_I_ID, inodeId));
    if(timestamp.isPresent()) {
      ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOpsAux.TIMESTAMP_LTE, timestamp));
    }
    DistributedFileSystemOps dfso = dfs.getDfsOps();
    Store store = new Hops(withArchiveProject, dfso);
    store.init();
    Archival archival = new Archival(store, Optional.of(inodeId));
    return provArchiveFilePrefix(archival, fileOpsFilters, Optional.empty());
  }
  
  public Long provCleanupFilePrefix(Long inodeId, Optional<Long> timestamp)
    throws GenericException, ServiceException {
    return provCleanupFilePrefix(inodeId, timestamp, Optional.empty());
  }
  
  public Long provCleanupFilePrefix(String docId, boolean skipDoc)
    throws ServiceException, GenericException {
    FileOp doc = getFileOp(docId);
    Optional<String> skipDocO = skipDoc ? Optional.of(docId) : Optional.empty();
    return provCleanupFilePrefix(doc.getInodeId(), Optional.of(doc.getTimestamp()), skipDocO);
  }
  
  private Long provCleanupFilePrefix(Long inodeId, Optional<Long> timestamp, Optional<String> skipDocO)
    throws GenericException, ServiceException {
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters = new HashMap<>();
    ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOps.FILE_I_ID, inodeId));
    if(timestamp.isPresent()) {
      ProvParamBuilder.addToFilters(fileOpsFilters, Pair.with(ProvFileQuery.FileOpsAux.TIMESTAMP_LTE, timestamp.get()));
    }
    Archival archival = new Archival(new NoStore(), Optional.empty());
    return provArchiveFilePrefix(archival, fileOpsFilters, skipDocO);
  }
  
  public void provArchiveProject(String docId, boolean skipDoc) throws ServiceException, GenericException {
    FileOp doc = getFileOp(docId);
    Optional<String> skipDocO = skipDoc ? Optional.of(docId) : Optional.empty();
    
  }
  
  private Long provArchiveFilePrefix(Archival archival, Map<String, ProvFileQuery.FilterVal> fileOpsFilters,
    Optional<String> skipDocO)
    throws ServiceException, GenericException {
    SearchRequest archivalRequest = archivalScrollingRequest(fileOpsFilters, skipDocO);
    ProvElasticHelper.ElasticComplexResultProcessor<Archival> processor =
      new ProvElasticHelper.ElasticComplexResultProcessor<>(archival, archivalConsumer);
    ProvElasticHelper.searchScrollingWithComplexAction(heClient, archivalRequest, processor);
    return archival.counter;
  }
  
  private SearchRequest archivalScrollingRequest(Map<String, ProvFileQuery.FilterVal> fileOpsFilters,
    Optional<String> skipDoc)
    throws GenericException {
    CheckedSupplier<SearchRequest, GenericException> srF =
      scrollingSearchRequest(
        Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE,
        HopsworksElasticClient.ARCHIVAL_PAGE_SIZE)
      .andThen(filterByArchival(fileOpsFilters, skipDoc))
      .andThen(sortByTimestamp());
    return srF.get();
  }
  
  public String archiveId(Long inodeId) {
    return inodeId + "-archive";
  }
  
  public ArchiveDTO.Base getArchive(Long inodeId) throws ServiceException, GenericException {
    GetRequest request = new GetRequest(
      Settings.ELASTIC_INDEX_FILE_PROVENANCE,
      Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE,
      archiveId(inodeId));
    ArchiveDTO.Base result = ProvElasticHelper.getFileDoc(heClient, request, ArchiveDTO.Base::instance);
    if(result == null) {
      result = new ArchiveDTO.Base();
      result.setInodeId(inodeId);
    }
    return result;
  }
  
  private void createArchive(Long inodeId)
    throws GenericException, ServiceException {
    GetRequest getBase = new GetRequest(Settings.ELASTIC_INDEX_FILE_PROVENANCE,
      Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE,
      archiveId(inodeId));
    ArchiveDTO.Base baseArchival = ProvElasticHelper.getFileDoc(heClient, getBase, ArchiveDTO.Base::instance);
    if(baseArchival == null) {
      IndexRequest indexBase = new IndexRequest(Settings.ELASTIC_INDEX_FILE_PROVENANCE,
        Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE,
        archiveId(inodeId));
      Map<String, Object> docMap = new HashMap<>();
      docMap.put(ProvElasticFields.FileBase.INODE_ID.toString(), inodeId);
      docMap.put(ProvElasticFields.FileAux.ENTRY_TYPE.toString(),
        ProvElasticFields.EntryType.ARCHIVE.toString().toLowerCase());
      indexBase.source(docMap);
      ProvElasticHelper.indexDoc(heClient, indexBase);
    }
  }
  
  private void updateArchive(Long inodeId, String location, Long line)
    throws ServiceException {
    UpdateRequest updateBase = new UpdateRequest(
      Settings.ELASTIC_INDEX_FILE_PROVENANCE,
      Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE,
      archiveId(inodeId));
    Map<String, Object> updateMap = new HashMap<>();
    updateMap.put(ProvElasticFields.FileOpsBase.ARCHIVE_LOC.toString(), new String[]{location + ":" + line});
    updateBase.doc(updateMap);
    ProvElasticHelper.updateDoc(heClient, updateBase);
  }
  //****
  
  public FileOp getFileOp(String docId) throws ServiceException, GenericException {
    CheckedFunction<Map<String, Object>, FileOp, GenericException> opParser
      = sourceMap -> FileOp.instance(docId, sourceMap);
    GetRequest request = new GetRequest(Settings.ELASTIC_INDEX_FILE_PROVENANCE,
      Settings.ELASTIC_INDEX_FILE_PROVENANCE_DEFAULT_TYPE, docId);
    return ProvElasticHelper.getFileDoc(heClient, request, opParser);
  }
  
  private CheckedSupplier<SearchRequest, GenericException> baseSearchRequest(String index, String docType) {
    return () -> {
      SearchRequest sr = new SearchRequest(index)
        .types(docType);
      sr.source().size(HopsworksElasticClient.DEFAULT_PAGE_SIZE);
      return sr;
    };
  }
  
  private CheckedSupplier<SearchRequest, GenericException> scrollingSearchRequest(String index, String docType,
    int pageSize) {
    return () -> {
      SearchRequest sr = new SearchRequest(index)
        .types(docType)
        .scroll(TimeValue.timeValueMinutes(1));
      sr.source().size(pageSize);
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
    List<Pair<ProvFileQuery.Field, SortOrder>> fileStateSortBy, List<Pair<String, SortOrder>> xattrSortBy) {
    return (SearchRequest sr) -> {
      //      if(fileStateSortBy.isEmpty() && xattrSortBy.isEmpty()) {
      //        srb.addSort(SortBuilders.fieldSort("_doc").order(SortOrder.ASC));
      //      } else {
      for (Pair<ProvFileQuery.Field, SortOrder> sb : fileStateSortBy) {
        sr.source().sort(SortBuilders.fieldSort(sb.getValue0().elasticFieldName()).order(sb.getValue1()));
      }
      for (Pair<String, SortOrder> sb : xattrSortBy) {
        sr.source().sort(SortBuilders.fieldSort(sb.getValue0()).order(sb.getValue1()));
      }
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> withFileOpsOrder(
    List<Pair<ProvFileQuery.Field, SortOrder>> fileOpsSortBy) {
    return (SearchRequest sr) -> {
      for (Pair<ProvFileQuery.Field, SortOrder> sb : fileOpsSortBy) {
        sr.source().sort(SortBuilders.fieldSort(sb.getValue0().elasticFieldName()).order(sb.getValue1()));
      }
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> sortByTimestamp() {
    return (SearchRequest sr) -> {
      sr.source().sort(ProvElasticFields.FileOpsBase.TIMESTAMP.toString().toLowerCase(), SortOrder.ASC);
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> withAggregations(
    List<ProvElasticHelper.ProvAggregations> aggregations) {
    return (SearchRequest sr) -> {
      if(!aggregations.isEmpty()) {
        for (ProvElasticHelper.ProvAggregations aggregation : aggregations) {
          sr.source().aggregation(aggregation.aggregation);
        }
      }
      return sr;
    };
  }
  
  private ProvElasticHelper.ElasticBasicResultProcessor<List<FileState>> fileStateParser() {
    return new ProvElasticHelper.ElasticBasicResultProcessor<>(new LinkedList<>(),
      (SearchHit[] hits,  List<FileState> acc) -> {
        for (SearchHit rawHit :hits) {
          FileState hit = FileState.instance(rawHit);
          acc.add(hit);
        }
      });
  }
  
  private ProvElasticHelper.ElasticBasicResultProcessor<List<FileOp>> fileOpsParser() {
    return new ProvElasticHelper.ElasticBasicResultProcessor<>(new LinkedList<>(),
      (SearchHit[] hits,  List<FileOp> acc) -> {
        for (SearchHit rawHit : hits) {
          FileOp hit = FileOp.instance(rawHit);
          acc.add(hit);
        }
      });
  }
  
  private ProvElasticHelper.ElasticBasicResultProcessor<Map<String, Map<Provenance.AppState, AppProvenanceHit>>>
    appStateParser() {
    return new ProvElasticHelper.ElasticBasicResultProcessor<>(new HashMap<>(),
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
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> filterByStateParams(
    Map<String, ProvFileQuery.FilterVal> fileStateFilters,
    Map<String, String> xAttrsFilters, Map<String, String> likeXAttrsFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvElasticFields.FileAux.ENTRY_TYPE.toString().toLowerCase(),
          ProvElasticFields.EntryType.STATE.toString().toLowerCase()));
      query = filterByBasicFields(query, fileStateFilters);
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
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> filterByOpsParams(
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters, List<Script> scriptFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvElasticFields.FileAux.ENTRY_TYPE.toString().toLowerCase(),
          ProvElasticFields.EntryType.OPERATION.toString().toLowerCase()));
      query = filterByBasicFields(query, fileOpsFilters);
      query = filterByScripts(query, scriptFilters);
      sr.source().query(query);
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> filterByArchival(
    Map<String, ProvFileQuery.FilterVal> fileOpsFilters, Optional<String> skipDoc) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery()
        .must(termQuery(ProvElasticFields.FileAux.ENTRY_TYPE.toString().toLowerCase(),
          ProvElasticFields.EntryType.OPERATION.toString().toLowerCase()));
      query = filterByBasicFields(query, fileOpsFilters);
      if(skipDoc.isPresent()) {
        query.mustNot(idsQuery().addIds(skipDoc.get()));
      }
      sr.source().query(query);
      return sr;
    };
  }
  
  private CheckedFunction<SearchRequest, SearchRequest, GenericException> provAppStateQB(
    Map<String, ProvFileQuery.FilterVal> appStateFilters) {
    return (SearchRequest sr) -> {
      BoolQueryBuilder query = boolQuery();
      query = filterByBasicFields(query, appStateFilters);
      sr.source().query(query);
      return sr;
    };
  }
  
  private BoolQueryBuilder filterByBasicFields(BoolQueryBuilder query,
    Map<String, ProvFileQuery.FilterVal> filters) throws GenericException {
    for (Map.Entry<String, ProvFileQuery.FilterVal> fieldFilters : filters.entrySet()) {
      query.must(fieldFilters.getValue().query());
    }
    return query;
  }
  
  private BoolQueryBuilder filterByScripts(BoolQueryBuilder query, List<Script> filterScripts) {
    if(filterScripts.isEmpty()) {
      return query;
    }
    BoolQueryBuilder scriptQB = boolQuery();
    query.must(scriptQB);
    for(Script script : filterScripts) {
      scriptQB.must(new ScriptQueryBuilder(script));
    }
    return query;
  }
  
  public QueryBuilder getXAttrQB(String xattrAdjustedKey, String xattrVal) {
    return termQuery(xattrAdjustedKey, xattrVal.toLowerCase());
  }
  
  public QueryBuilder getLikeXAttrQB(String xattrAdjustedKey, String xattrVal) {
    return ProvElasticHelper.fullTextSearch(xattrAdjustedKey, xattrVal);
  }
}
