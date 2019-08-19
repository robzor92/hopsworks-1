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
package io.hops.hopsworks.api.provenance;

import io.hops.hopsworks.api.filter.NoCacheResponse;
import io.hops.hopsworks.common.provenance.AppFootprintType;
import io.hops.hopsworks.common.provenance.ProvFileOpHit;
import io.hops.hopsworks.common.provenance.ProvFileOpsCompactByApp;
import io.hops.hopsworks.common.provenance.ProvFileOpsSummaryByApp;
import io.hops.hopsworks.common.provenance.v2.xml.FileState;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateResult;
import io.hops.hopsworks.common.provenance.v2.xml.FileStateTree;
import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileState;
import io.hops.hopsworks.common.provenance.ProvenanceController;
import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileStateResult;
import io.hops.hopsworks.common.provenance.v2.xml.SimpleResult;
import io.hops.hopsworks.common.provenance.v2.ProvFileOpsParamBuilder;
import io.hops.hopsworks.common.provenance.v2.ProvFileStateParamBuilder;
import io.hops.hopsworks.common.provenance.v2.xml.FootprintFileStateTree;
import io.hops.hopsworks.exceptions.GenericException;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.javatuples.Pair;

import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class ProvenanceResourceHelper {
  public static Response getFileStates(NoCacheResponse noCacheResponse, ProvenanceController provenanceCtrl,
    ProvFileStateParamBuilder params, ProjectProvenanceResource.FileStructReturnType returnType)
    throws GenericException, ServiceException {
    switch(returnType) {
      case LIST:
        Map<Long,FileState> listAux = provenanceCtrl.provFileStateList(params);
        FileStateResult.List listResult
          = new FileStateResult.List(listAux.values());
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(listResult).build();
      case MIN_TREE:
        Pair<Map<Long, FileStateTree>, Map<Long, FileStateTree>> minAux
          = provenanceCtrl.provFileStateTree(params, false);
        FileStateResult.MinTree minTreeResult
          = new FileStateResult.MinTree(minAux.getValue0().values());
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(minTreeResult).build();
      case FULL_TREE:
        Pair<Map<Long, FileStateTree>, Map<Long, FileStateTree>> fullAux
          = provenanceCtrl.provFileStateTree(params, true);
        FileStateResult.FullTree fullTreeResult
          = new FileStateResult.FullTree(fullAux.getValue0().values(), fullAux.getValue1().values());
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(fullTreeResult).build();
      case COUNT:
        Long countResult = provenanceCtrl.provFileStateCount(params);
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
          .entity(new SimpleResult<>(countResult)).build();
      default:
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
          "return type: " + returnType + " is not managed");
    }
  }
  
  public static Response getAppFootprint(NoCacheResponse noCacheResponse, ProvenanceController provenanceCtrl,
    ProvFileOpsParamBuilder params, AppFootprintType footprintType,
    ProjectProvenanceResource.FileStructReturnType returnType)
    throws GenericException, ServiceException {
    switch(returnType) {
      case LIST:
        Map<Long, FootprintFileState> listAux = provenanceCtrl.provAppFootprintList(params, footprintType);
        FootprintFileStateResult.List listResult
          = new FootprintFileStateResult.List(listAux.values());
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(listResult).build();
      case MIN_TREE:
        Pair<Map<Long, FootprintFileStateTree>, Map<Long, FootprintFileStateTree>> minAux
          = provenanceCtrl.provAppFootprintTree(params, footprintType, false);
        FootprintFileStateResult.MinTree minTreeResult
          = new FootprintFileStateResult.MinTree(minAux.getValue0().values());
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(minTreeResult).build();
      case FULL_TREE:
        Pair<Map<Long, FootprintFileStateTree>, Map<Long, FootprintFileStateTree>> fullAux
          = provenanceCtrl.provAppFootprintTree(params, footprintType,true);
        FootprintFileStateResult.FullTree fullTreeResult
          = new FootprintFileStateResult.FullTree(fullAux.getValue0().values(), fullAux.getValue1().values());
        return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK).entity(fullTreeResult).build();
      case COUNT:
      default:
        throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
          "return type: " + returnType + " is not managed");
    }
  }
  
  
  public static Response getFileOps(NoCacheResponse noCacheResponse, ProvenanceController provenanceCtrl,
    ProvFileOpsParamBuilder params, ProjectProvenanceResource.FileOpsCompactionType opsCompaction,
    ProjectProvenanceResource.FileStructReturnType returnType)
    throws ServiceException, GenericException {
    if(ProjectProvenanceResource.FileStructReturnType.COUNT.equals(returnType)) {
      Long result = provenanceCtrl.provFileOpsCount(params);
      return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
        .entity(new SimpleResult<>(result)).build();
    } else {
      List<ProvFileOpHit> result = provenanceCtrl.provFileOpsList(params);
      switch(opsCompaction) {
        case NONE:
          return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
            .entity(new GenericEntity<List<ProvFileOpHit>>(result) {}).build();
        case COMPACT:
          List<ProvFileOpsCompactByApp> compactResults = ProvFileOpsCompactByApp.compact(result);
          return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
            .entity(new GenericEntity<List<ProvFileOpsCompactByApp>>(compactResults) {}).build();
        case SUMMARY:
          List<ProvFileOpsSummaryByApp> summaryResults = ProvFileOpsSummaryByApp.summary(result);
          return noCacheResponse.getNoCacheResponseBuilder(Response.Status.OK)
            .entity(new GenericEntity<List<ProvFileOpsSummaryByApp>>(summaryResults) {}).build();
        default:
          throw new GenericException(RESTCodes.GenericErrorCode.ILLEGAL_ARGUMENT, Level.WARNING,
            "footprint filterType: " + returnType);
      }
    }
  }
}
