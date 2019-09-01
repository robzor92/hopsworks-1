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

import io.hops.hopsworks.common.util.Ip;
import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.exceptions.ServiceException;
import io.hops.hopsworks.restutils.RESTCodes;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An Elastic transport client shared by elastic controllers in hopsworks.
 * An Elastic transport client opens quite a few tcp connection that are kept alive for possibly minutes, and in test
 * cases for 50-100 parallel ongoing hopsworks elastic based requests it end up with upwards of 2000 tcp connections
 * used by elastic clients - this can also saturate the amount of tcp connection configured by the elastic node
 * ending up with a NoNodeAvailableException
 */
@Singleton
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
public class HopsworksElasticClient {

  private static final Logger LOG = Logger.getLogger(HopsworksElasticClient.class.getName());
  public static final Integer DEFAULT_PAGE_SIZE = 1000;
  public static final Integer MAX_PAGE_SIZE = 10000;

  @EJB
  private Settings settings;

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

  /**
   * Shuts down the client
   * <p/>
   */
  private void shutdownClient() {
    if (elasticClient != null) {
      elasticClient.close();
      elasticClient = null;
    }
  }
  
  private Client getClient() throws ServiceException {
    if (elasticClient == null) {
      LOG.log(Level.INFO, "creating new elastic client");
      final org.elasticsearch.common.settings.Settings settings
        = org.elasticsearch.common.settings.Settings.builder()
        .put("client.transport.sniff", true) //being able to retrieve other nodes
        .put("cluster.name", "hops").build();
      
      elasticClient = new PreBuiltTransportClient(settings).addTransportAddress(
        new TransportAddress(new InetSocketAddress(getElasticIpAsString(), this.settings.getElasticPort())));
      
      Iterator<ThreadPool.Info> tpInfoIt = elasticClient.threadPool().info().iterator();
      StringBuilder tp = new StringBuilder();
      while(tpInfoIt.hasNext()) {
        ThreadPool.Info tpInfo = tpInfoIt.next();
        switch(tpInfo.getName()) {
          case ThreadPool.Names.BULK:
          case ThreadPool.Names.INDEX:
          case ThreadPool.Names.SEARCH:
            tp.append("name:").append(tpInfo.getName())
              .append("(")
              .append(tpInfo.getThreadPoolType())
              .append(",")
              .append(tpInfo.getMin())
              .append(",")
              .append(tpInfo.getMax())
              .append(",")
              .append(tpInfo.getQueueSize().singles())
              .append(")");
        }
      }
      LOG.log(Level.INFO, "threadpools {0}", tp);
    }
    return elasticClient;
  }
  
  private String getElasticIpAsString() throws ServiceException {
    String addr = settings.getElasticIp();

    // Validate the ip address pulled from the variables
    if (!Ip.validIp(addr)) {
      try {
        InetAddress.getByName(addr);
      } catch (UnknownHostException ex) {
        throw new ServiceException(RESTCodes.ServiceErrorCode.ELASTIC_SERVER_NOT_AVAILABLE,
          Level.SEVERE, null, ex.getMessage(), ex);
      }
    }
    return addr;
  }
  
  public ActionFuture<SearchResponse> search(SearchRequest request) throws ServiceException {
    return getClient().search(request);
  }
  
  public  ActionFuture<SearchResponse> searchScroll(SearchScrollRequest request) throws ServiceException {
    return getClient().searchScroll(request);
  }
  
  public ActionFuture<BulkResponse> bulkOp(BulkRequest request) throws ServiceException {
    return getClient().bulk(request);
  }
  
  public void processException(ServiceException ex) {
    LOG.log(Level.WARNING, "elastic client exception", ex);
    shutdownClient();
  }
}

