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
package io.hops.hopsworks.common.provenance;

import io.hops.hopsworks.common.dao.dataset.Dataset;
import io.hops.hopsworks.common.dao.hdfs.inode.Inode;
import io.hops.hopsworks.common.dao.hdfs.inode.InodeFacade;
import io.hops.hopsworks.common.dao.project.Project;
import io.hops.hopsworks.common.elastic.ElasticController;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.DistributedFsService;
import io.hops.hopsworks.common.hdfs.Utils;
import io.hops.hopsworks.common.provenance.v2.ProvFileOps;
import io.hops.hopsworks.common.provenance.v2.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateTree;
import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileState;
import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileStateTree;
import io.hops.hopsworks.common.provenance.v2.xml.TreeHelper;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.javatuples.Pair;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Level;

@Stateless
public class ProvenanceController {
  @EJB
  private ElasticController elasticCtrl;
  @EJB
  private DistributedFsService dfs;
  @EJB
  private InodeFacade inodeFacade;
  
  public static final String PROJECT_PROVENANCE_STATUS_XATTR_NAME = "provenance.meta_status";
  
  public boolean isProjectProvenanceEnabled(Project project) throws GenericException {
    return getProjectProvenanceStatus(project).equals(Inode.MetaStatus.PROVENANCE_ENABLED);
  }
  
  public boolean isProjectProvenanceEnabled(Project project, DistributedFileSystemOps dfso) throws GenericException {
    return getProjectProvenanceStatus(project, dfso).equals(Inode.MetaStatus.PROVENANCE_ENABLED);
  }
  
  public Inode.MetaStatus getProjectProvenanceStatus(Project project) throws GenericException {
    DistributedFileSystemOps dfso = dfs.getDfsOps();
    try {
      return getProjectProvenanceStatus(project, dfso);
    } finally {
      if (dfso != null) {
        dfso.close();
      }
    }
  }
  
  public Inode.MetaStatus getProjectProvenanceStatus(Project project, DistributedFileSystemOps dfso)
    throws GenericException {
    String projectPath = Utils.getProjectPath(project.getName());
    try {
      byte[] bVal = dfso.getXAttr(projectPath, PROJECT_PROVENANCE_STATUS_XATTR_NAME);
      Inode.MetaStatus status;
      if(bVal == null) {
        status = Inode.MetaStatus.DISABLED;
      } else {
        status = Inode.MetaStatus.valueOf(new String(bVal));
      }
      return status;
    } catch (IOException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "project provenance xattr persistance exception");
    }
  }
  
  public void changeProjectProvenanceStatus(Project project, Inode.MetaStatus newStatus) throws GenericException {
    String projectPath = Utils.getProjectPath(project.getName());
    DistributedFileSystemOps dfso = dfs.getDfsOps();
    try {
      byte[] bVal = dfso.getXAttr(projectPath, PROJECT_PROVENANCE_STATUS_XATTR_NAME);
      Inode.MetaStatus previousStatus;
      if(bVal == null) {
        previousStatus = Inode.MetaStatus.DISABLED;
      } else {
        previousStatus = Inode.MetaStatus.valueOf(new String(bVal));
      }
      
      if(newStatus.equals(previousStatus)) {
        return;
      }
      if(Inode.MetaStatus.DISABLED.equals(previousStatus)) {
        upgradeDatasetsMetaStatus(project, dfso);
        dfso.insertXAttr(projectPath, PROJECT_PROVENANCE_STATUS_XATTR_NAME, newStatus.name().getBytes());
      } else {
        downgradeDatasetsMetaStatus(project, dfso);
        dfso.removeXAttr(projectPath, PROJECT_PROVENANCE_STATUS_XATTR_NAME);
      }
    } catch (IOException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "project provenance xattr persistance exception");
    } finally {
      if (dfso != null) {
        dfso.close();
      }
    }
  }
  
  public List<ProvDatasetState> getDatasetsProvenanceStatus(Project project) {
    List<ProvDatasetState> result = new ArrayList<>();
    for (Dataset ds : project.getDatasetCollection()) {
      ProvDatasetState dsState = new ProvDatasetState(ds.getName(), ds.getInode().getId(),
        ds.getInode().getMetaStatus());
      result.add(dsState);
    }
    return result;
  }
  
  public Map<Long, FileState> provFileStateList(ProvFileStateParamBuilder params)
    throws GenericException, ServiceException {
    Map<Long, FileState> result = new HashMap<>();
    Map<Long, FileState> fileStates = elasticCtrl.provFileState(
      params.getFileStateFilter(), params.getFileStateSortBy(),
      params.getExactXAttrFilter(), params.getLikeXAttrFilter());
    if (params.hasExpansionWithAppState()) {
      //If withAppStates, update params based on appIds of result files and do a appState index query.
      //After this filter the fileStates based on the results of the appState query
      for (FileState fileState : fileStates.values()) {
        params.withAppStateAppId(getAppId(fileState));
      }
      Map<String, Map<Provenance.AppState, AppProvenanceHit>> appStates
        = elasticCtrl.provAppState(params.getAppStateFilter());
      for(FileState fileState : fileStates.values()) {
        String appId = getAppId(fileState);
        if(appStates.containsKey(appId)) {
          Map<Provenance.AppState, AppProvenanceHit> auxAppStates = appStates.get(appId);
          fileState.setAppState(buildAppState(auxAppStates));
          result.put(fileState.getInodeId(), fileState);
        }
      }
    } else {
      result.putAll(fileStates);
    }
    return result;
  }
  
  public Pair<Map<Long, FileStateTree>, Map<Long, FileStateTree>> provFileStateTree(
    ProvFileStateParamBuilder params, boolean fullTree)
    throws GenericException, ServiceException {
    Map<Long, FileState> fileStates = provFileStateList(params);
    Pair<Map<Long, ProvenanceController.BasicTreeBuilder<FileState>>,
      Map<Long, ProvenanceController.BasicTreeBuilder<FileState>>> result
      = processAsTree(fileStates, () -> new FileStateTree(), fullTree);
    return Pair.with((Map<Long, FileStateTree>)(Map)result.getValue0(),
      (Map<Long, FileStateTree>)(Map)(result.getValue1()));
  }
  
  public long provFileStateCount(ProvFileStateParamBuilder params)
    throws GenericException, ServiceException {
    if(params.hasExpansionWithAppState()) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "provenance file state count does not currently work with app state expansion");
    }
    return elasticCtrl.provFileStateCount(params.getFileStateFilter(), params.getExactXAttrFilter(),
      params.getLikeXAttrFilter());
  }
  
  public List<ProvFileOpHit> provFileOpsList(ProvFileOpsParamBuilder params)
    throws GenericException, ServiceException {
    return elasticCtrl.provFileOps(params.getFileOpsFilter());
  }
  
  public long provFileOpsCount(ProvFileOpsParamBuilder params)
    throws ServiceException, GenericException {
    return elasticCtrl.provFileOpsCount(params.getFileOpsFilter());
  }
  
  public Map<Long, FootprintFileState> provAppFootprintList(ProvFileOpsParamBuilder params,
    AppFootprintType footprintType)
    throws GenericException, ServiceException {
    addAppFootprintFileOps(params, footprintType);
    List<ProvFileOpHit> searchResult = provFileOpsList(params);
    Map<Long, FootprintFileState> appFootprint = processAppFootprintFileOps(searchResult, footprintType);
    return appFootprint;
  }
  
  public Pair<Map<Long, FootprintFileStateTree>, Map<Long, FootprintFileStateTree>> provAppFootprintTree(
    ProvFileOpsParamBuilder params, AppFootprintType footprintType, boolean fullTree)
    throws GenericException, ServiceException {
    Map<Long, FootprintFileState> fileStates = provAppFootprintList(params, footprintType);
    Pair<Map<Long, ProvenanceController.BasicTreeBuilder<FootprintFileState>>,
      Map<Long, ProvenanceController.BasicTreeBuilder<FootprintFileState>>> result
      = processAsTree(fileStates, () -> new FootprintFileStateTree(), fullTree);
    return Pair.with((Map<Long, FootprintFileStateTree>)(Map)result.getValue0(),
      (Map<Long, FootprintFileStateTree>)(Map)(result.getValue1()));
  }
  
  public ProvFileOpsParamBuilder elasticPathQueryParams(List<Long> inodeIds) {
    ProvFileOpsParamBuilder params = new ProvFileOpsParamBuilder()
      .withFileOperation(ProvFileOps.CREATE)
      .withFileOperation(ProvFileOps.DELETE);
    for(Long inodeId : inodeIds) {
      params.withFileInodeId(inodeId);
    }
    return params;
  }
  
  private void addAppFootprintFileOps(ProvFileOpsParamBuilder params, AppFootprintType footprintType) {
    switch(footprintType) {
      case ALL:
        break;
      case INPUT:
        params
          .withFileOperation(ProvFileOps.CREATE)
          .withFileOperation(ProvFileOps.ACCESS_DATA);
        break;
      case OUTPUT:
        params
          .withFileOperation(ProvFileOps.CREATE)
          .withFileOperation(ProvFileOps.MODIFY_DATA)
          .withFileOperation(ProvFileOps.DELETE);
        break;
      case OUTPUT_ADDED:
      case TMP:
      case REMOVED:
        params
          .withFileOperation(ProvFileOps.CREATE)
          .withFileOperation(ProvFileOps.DELETE);
        break;
      default:
        throw new IllegalArgumentException("footprint filterType:" + footprintType + " not managed");
    }
  }
  
  private Map<Long, FootprintFileState> processAppFootprintFileOps(List<ProvFileOpHit> fileOps,
    AppFootprintType footprintType) {
    Map<Long, FootprintFileState> files = new HashMap<>();
    Set<Long> filesAccessed = new HashSet<>();
    Set<Long> filesCreated = new HashSet<>();
    Set<Long> filesModified = new HashSet<>();
    Set<Long> filesDeleted = new HashSet<>();
    for(ProvFileOpHit fileOp : fileOps) {
      files.put(fileOp.getInodeId(), new FootprintFileState(fileOp.getInodeId(), fileOp.getInodeName(),
        fileOp.getParentInodeId(), fileOp.getProjectInodeId()));
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
    return files;
  }
  
  private <S extends ProvenanceController.BasicFileState>
    Pair<Map<Long, ProvenanceController.BasicTreeBuilder<S>>, Map<Long, ProvenanceController.BasicTreeBuilder<S>>>
    processAsTree(Map<Long, S> fileStates, Supplier<ProvenanceController.BasicTreeBuilder<S>> instanceBuilder,
    boolean fullTree)
    throws GenericException, ServiceException {
    TreeHelper.TreeStruct<S> treeS = new TreeHelper.TreeStruct<>(instanceBuilder);
    treeS.processBasicFileState(fileStates);
    if(fullTree) {
      int maxDepth = 100;
      while(!treeS.complete() && maxDepth > 0 ) {
        maxDepth--;
        while (treeS.findInInodes()) {
          List<Long> inodeIdBatch = treeS.nextFindInInodes();
          List<Inode> inodeBatch = inodeFacade.findByIdList(inodeIdBatch);
          treeS.processInodeBatch(inodeIdBatch, inodeBatch);
        }
        while (treeS.findInProvenance()) {
          List<Long> inodeIdBatch = treeS.nextFindInProvenance();
          ProvFileOpsParamBuilder elasticPathQueryParams = elasticPathQueryParams(inodeIdBatch);
          List<ProvFileOpHit> inodeBatch = elasticCtrl.provFileOps(elasticPathQueryParams.getFileOpsFilter());
          treeS.processProvenanceBatch(inodeIdBatch, inodeBatch);
        }
      }
      return treeS.getFullTree();
    } else {
      return treeS.getMinTree();
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
  
  private String getAppId(FileState fileState) {
    if(fileState.getAppId().equals("notls")) {
      if(fileState.getXattrs().containsKey("appId")) {
        return fileState.getXattrs().get("appId");
      } else {
        throw new IllegalArgumentException("withAppId enabled for tls clusters or notls cluster with xattr appIds");
      }
    } else {
      return fileState.getAppId();
    }
  }
  
  private void upgradeDatasetsMetaStatus(Project project, DistributedFileSystemOps dfso) throws GenericException {
    try {
      for (Dataset ds : project.getDatasetCollection()) {
        if(isHive(ds) || isFeatureStore(ds)) {
          //TODO - bug?
          continue;
        }
        if (Inode.MetaStatus.META_ENABLED.equals(ds.getInode().getMetaStatus())) {
          String datasetPath = Utils.getDatasetPath(project.getName(), ds.getName());
          dfso.setProvenanceEnabled(datasetPath);
        }
      }
    } catch (IOException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "dataset provenance persistance exception");
    }
  }
  
  private void downgradeDatasetsMetaStatus(Project project, DistributedFileSystemOps dfso) throws GenericException {
    try {
      for (Dataset ds : project.getDatasetCollection()) {
        if(isHive(ds) || isFeatureStore(ds)) {
          //TODO - bug?
          continue;
        }
        if (Inode.MetaStatus.PROVENANCE_ENABLED.equals(ds.getInode().getMetaStatus())) {
          String datasetPath = Utils.getDatasetPath(project.getName(), ds.getName());
          dfso.setMetaEnabled(datasetPath);
        }
      }
    } catch (IOException e) {
      throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_STATE, Level.INFO,
        "dataset provenance persistance exception");
    }
  }
  
  private boolean isHive(Dataset ds) {
    String hiveDB = ds.getProject().getName() + ".db";
    return hiveDB.equals(ds.getName());
  }
  
  private boolean isFeatureStore(Dataset ds) {
    String hiveDB = ds.getProject().getName() + "_featurestore.db";
    return hiveDB.equals(ds.getName());
  }
  
  public interface BasicFileState {
    Long getInodeId();
    String getInodeName();
    Long getProjectInodeId();
    boolean isProject();
    Long getParentInodeId();
  }
  
  public interface BasicTreeBuilder<S extends BasicFileState> {
    void setInodeId(Long inodeId);
    Long getInodeId();
    void setName(String name);
    String getName();
    void setFileState(S fileState);
    S getFileState();
    void addChild(BasicTreeBuilder<S> child) throws GenericException;
  }
}
