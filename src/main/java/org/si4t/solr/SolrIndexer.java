/**
 * Copyright 2011-2013 Radagio & SDL
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.si4t.solr;

import com.tridion.configuration.Configuration;
import com.tridion.configuration.ConfigurationException;
import com.tridion.storage.si4t.*;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.si4t.solr.SolrClientRequest.ServerMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SolrIndexer.
 * <p>
 * TODO: Make this completely transactional as well.
 *
 * @author R.S. Kempees
 */
public class SolrIndexer implements SearchIndex {
    private static final Logger LOG = LoggerFactory.getLogger(SolrIndexer.class);
    private String solrHome = null;
    private String coreName = null;
    private String solrServerUrl = null;
    private SolrClientRequest.ServerMode solrServerMode = SolrClientRequest.ServerMode.HTTP;
    private List<Configuration> solrCores = null;
    private List<Configuration> solrUrls = null;
    private String defaultCoreUrl = null;
    private ConcurrentHashMap<String, BaseIndexData> itemRemovals = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, SearchIndexData> itemAdds = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, BinaryIndexData> binaryAdds = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, SearchIndexData> itemUpdates = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> solrUrlMap = new ConcurrentHashMap<>();

    private void setCoreNameAndSolrHome(String pubId) throws ConfigurationException {
        if (this.solrCores != null && !Utils.StringIsNullOrEmpty(pubId)) {
            for (Configuration core : this.solrCores) {
                if (core.hasAttribute("Id")) {
                    if (core.getAttribute("Id") != null) {
                        if (core.getAttribute("Id").equalsIgnoreCase(pubId)) {
                            if (core.getAttribute("Name") != null) {
                                this.coreName = core.getAttribute("Name");
                                LOG.info("Using [" + this.coreName + "] as Solr core.");

                            }
                            if (core.getAttribute("SolrHome") != null) {
                                this.solrHome = core.getAttribute("SolrHome");
                                LOG.info("Using [" + this.solrHome + "] as Solr Home.");
                                return;
                            }
                        }
                    }
                }
            }
        }
        throw new ConfigurationException("Could not find a Solr core or Solr Home for Publication Id: " + pubId);
    }

    /*
     * (non-Javadoc)
     * @see
     * com.tridion.storage.si4t.SearchIndex#configure(com.tridion
     * .configuration.Configuration)
     */

    @Override
    public void configure(Configuration configuration) throws ConfigurationException {
        LOG.debug("Version 1.3.1-logging");
        LOG.debug("Configuration is: " + configuration.toString());
        Configuration indexerConfiguration = configuration.getChild("Indexer");

        String requestMode = indexerConfiguration.getAttribute("Mode");
        if (Utils.StringIsNullOrEmpty(requestMode)) {
            throw new ConfigurationException("Missing Mode attribute. Must either be 'embedded' or 'http'. Exiting");
        }

        if (requestMode.equalsIgnoreCase("http")) {
            this.solrServerMode = ServerMode.HTTP;
            List<Configuration> solrUrls = indexerConfiguration.getChild("Urls").getChildren();

            if (solrUrls != null) {
                for (Configuration url : solrUrls) {
                    if (url.hasAttribute("Id")) {
                        if (url.getAttribute("Id") != null) {
                            solrUrlMap.put(url.getAttribute("Id"), url.getAttribute("Value"));
                        }
                    }
                }
            }

            if (indexerConfiguration.hasAttribute("DefaultCoreUrl")) {
                String defaultCoreUrl = indexerConfiguration.getAttribute("DefaultCoreUrl");

                if (!Utils.StringIsNullOrEmpty(defaultCoreUrl)) {
                    LOG.info("Setting defaultCoreUrl to: " + defaultCoreUrl);
                    solrUrlMap.put("_DEFAULT_", defaultCoreUrl);
                }
            }

            if (solrUrlMap.isEmpty()) {
                throw new ConfigurationException("Request mode is set to HTTP, but no valid Url collection or the DefaultCoreUrl is present. Set the Urls collection in the Indexer configuration node, or add the DeafultCoreAttribute to the IndexerConfiguration");
            }

        } else if (requestMode.equalsIgnoreCase("embedded")) {
            throw new ConfigurationException("Request mode is set to embedded, but this deprecated feature is now removed. Please use the HTTP option.");
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * com.tridion.storage.si4t.SearchIndex#addBinaryToIndex(com
     * .tridion.extensions.storage.search.SearchIndexData)
     */
    @Override
    public void addBinaryToIndex(BinaryIndexData data) throws IndexingException {
        if (Utils.StringIsNullOrEmpty(data.getUniqueIndexId())) {
            LOG.error("Addition failed. Unique ID is empty");
            return;
        }
        this.binaryAdds.put(data.getUniqueIndexId(), data);
    }

    /*
     * (non-Javadoc)
     * @see
     * com.tridion.storage.si4t.SearchIndex#addPageToIndex(java
     * .lang.String, org.w3c.dom.Document)
     */
    @Override
    public void addItemToIndex(SearchIndexData data) throws IndexingException {
        if (Utils.StringIsNullOrEmpty(data.getUniqueIndexId())) {
            LOG.error("Addition failed. Unique ID is empty");
            return;
        }

        if (data.getFieldSize() == 0) {
            LOG.warn("To be indexed item has no data.");
            LOG.warn("Item is: " + data.toString());
        }

        if (!this.itemAdds.containsKey(data.getUniqueIndexId())) {
            this.itemAdds.put(data.getUniqueIndexId(), data);
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * com.tridion.storage.si4t.SearchIndex#removeBinaryFromIndex
     * (com.tridion.storage.si4t.SearchIndexData)
     */
    @Override
    public void removeBinaryFromIndex(BaseIndexData data) throws IndexingException {
        if (Utils.StringIsNullOrEmpty(data.getUniqueIndexId())) {
            LOG.error("Removal addition failed. Unique ID empty");
            return;
        }
        this.itemRemovals.put(data.getUniqueIndexId(), data);
    }

    /*
     * (non-Javadoc)
     * @see
     * com.tridion.storage.si4t.SearchIndex#removePageFromIndex
     * (com.tridion.storage.si4t.SearchIndexData)
     */
    @Override
    public void removeItemFromIndex(BaseIndexData data) throws IndexingException {
        LOG.info("Adding removeItemFromIndex: " + data.toString());
        if (Utils.StringIsNullOrEmpty(data.getUniqueIndexId())) {
            LOG.error("Removal addition failed. Unique ID empty");
            return;
        }

        this.itemRemovals.put(data.getUniqueIndexId(), data);
    }

    /*
     * (non-Javadoc)
     * @see
     * com.tridion.storage.si4t.SearchIndex#updateItemInIndex(com
     * .tridion.extensions.storage.search.SearchIndexData)
     */
    @Override
    public void updateItemInIndex(SearchIndexData data) throws IndexingException {
        if (Utils.StringIsNullOrEmpty(data.getUniqueIndexId())) {
            LOG.error("Adding update item failed. Unique ID empty");
            return;
        }
        this.itemUpdates.put(data.getUniqueIndexId(), data);

    }


    private String getSolrUrl(String publicationId) throws IndexingException {
        if (solrUrlMap.containsKey(publicationId)) {
            return solrUrlMap.get(publicationId);
        }
        if (solrUrlMap.containsKey("_DEFAULT_")) {
            return solrUrlMap.get("_DEFAULT_");
        }
        throw new IndexingException("Solr Server Exception: no solr url found for publication " + publicationId);
    }

    /*
     * (non-Javadoc)
     * @see com.tridion.storage.si4t.SearchIndex#commit()
     */
    @Override
    public void commit(String publicationId) throws IndexingException {
        try {

            String solrUrl = getSolrUrl(publicationId);
            LOG.debug("found solr url " + solrUrl + " for publication ID " + publicationId);
            String listOfUris = "";
            for (Map.Entry<String,SearchIndexData> entry : itemAdds.entrySet()) {
                LOG.debug("itemsToAdd contains: " + entry.getKey() + ", item id " + entry.getValue().getUniqueIndexId());
            }
            for (Map.Entry<String,SearchIndexData> entry : itemUpdates.entrySet()) {
                listOfUris += entry.getValue().getUniqueIndexId() + ",";
            }
            LOG.debug("itemUpdates contains: " + listOfUris);

            this.commitAddContentToSolr(this.itemAdds, solrUrl);
            this.commitAddBinariesToSolr(solrUrl);
            this.removeItemsFromSolr(solrUrl);
            this.processItemUpdates(solrUrl);

        } catch (SolrServerException e) {
            LOG.error(e.getLocalizedMessage(), e);
            throw new IndexingException("Solr Server Exception: " + e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage(), e);
            throw new IndexingException("IO Exception: " + e.getMessage());
        } catch (ParserConfigurationException e) {
            LOG.error(e.getLocalizedMessage(), e);
            throw new IndexingException("ParserConfigurationException: " + e.getMessage());
        } catch (SAXException e) {
            LOG.error(e.getLocalizedMessage(), e);
            throw new IndexingException("SAXException:" + e.getMessage());
        } catch (Throwable e) {
            LOG.error("Unexpected exception: " + e.getLocalizedMessage(), e);
            throw new IndexingException("Unexpected exception:" + e.getMessage());
        } finally {
            LOG.info("Clearing out registers.");
            this.clearRegisters();
        }
    }

    private void clearRegisters() {
        itemAdds.clear();
        LOG.debug("cleared item register");
        binaryAdds.clear();
        LOG.debug("cleared binary register");
        itemRemovals.clear();
        LOG.debug("cleared removal register");
        itemUpdates.clear();
        LOG.debug("cleared update register");
    }

    /**
     * Process item updates.
     * <p>
     * Taken into account the fact that with publishing a Tridion item
     * the full document is to be indexed,
     * an update is essentially the same as an addition.
     * Partial updates can be implemented here if the need arises
     *
     * @throws ParserConfigurationException
     * @throws IOException
     * @throws SAXException
     * @throws SolrServerException
     */
    private void processItemUpdates(String solrUrl) throws ParserConfigurationException, IOException, SAXException, SolrServerException {
        this.commitAddContentToSolr(this.itemUpdates, solrUrl);
    }

    private void commitAddBinariesToSolr(String solrUrl) throws SolrServerException, IOException, ParserConfigurationException, SAXException, IndexingException {
        if (this.binaryAdds.size() > 0) {
            LOG.info("Adding binaries to Solr.");

            LOG.info
                    (
                            SolrIndexDispatcher.INSTANCE.
                                    addBinaries(binaryAdds,
                                            new SolrClientRequest(
                                                    this.solrHome + "-" + this.coreName,
                                                    this.solrHome,
                                                    this.coreName,
                                                    solrUrl,
                                                    this.solrServerMode)
                                    )
                    );
        }
    }

    private void commitAddContentToSolr(ConcurrentHashMap<String, SearchIndexData> itemsToAdd, String solrUrl) throws SolrServerException, IOException, ParserConfigurationException, SAXException {
        int itemAddsize = itemsToAdd.size();
        if (itemAddsize > 0) {
            LOG.info("Adding pages and component presentations to Solr in batches of 10");

            ArrayList<ArrayList<SolrInputDocument>> groupedDocuments = new ArrayList<>();
            int step = 0;
            int i = 0;

            groupedDocuments.add(new ArrayList<>());
            for (Map.Entry<String, SearchIndexData> entry : itemsToAdd.entrySet()) {
                if (i % 10 == 0 && itemAddsize > 10) {
                    step++;
                    groupedDocuments.add(new ArrayList<>());
                }
                SearchIndexData data = entry.getValue();
                groupedDocuments.get(step).add(constructInputDocument(data, LOG));
                i++;
            }
            LOG.trace(groupedDocuments.toString());
            this.dispatchAddContentToSolr(groupedDocuments, solrUrl);
        }
    }

    private void dispatchAddContentToSolr(ArrayList<ArrayList<SolrInputDocument>> groupedDocuments, String solrUrl) throws ParserConfigurationException, IOException, SAXException, SolrServerException {
        LOG.info("Dispatching documents in " + groupedDocuments.size() + " steps.");

        for (ArrayList<SolrInputDocument> documents : groupedDocuments) {
            if (documents.size() > 0) {
                DispatcherPackage dispatcherPackage = new DispatcherPackage
                        (
                                DispatcherAction.PERSIST,
                                new SolrClientRequest(
                                        this.solrHome + "-" + this.coreName,
                                        this.solrHome,
                                        this.coreName,
                                        solrUrl,
                                        this.solrServerMode),
                                documents
                        );
                LOG.info
                        (
                                SolrIndexDispatcher.INSTANCE.addDocuments(dispatcherPackage)
                        );
            }
        }
    }

    private static SolrInputDocument constructInputDocument(SearchIndexData data, Logger log) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("pubdate", "NOW");
        log.info("Adding Id: " + data.getUniqueIndexId());
        doc.addField("id", data.getUniqueIndexId());

        ConcurrentHashMap<String, ArrayList<Object>> fieldList = data.getIndexFields();

        for (Entry<String, ArrayList<Object>> fieldEntry : fieldList.entrySet()) {
            String fieldName = fieldEntry.getKey();
            for (Object o : fieldEntry.getValue()) {
                doc.addField(fieldName, o);
                log.trace("Adding: " + fieldName + ": " + o);
            }
        }
        return doc;
    }

    private void removeItemsFromSolr(String solrUrl) throws SolrServerException, IOException, ParserConfigurationException, SAXException, IndexingException {
        if (this.itemRemovals.size() > 0) {
            LOG.info
                    (
                            SolrIndexDispatcher.INSTANCE.removeFromSolr(
                                    this.itemRemovals.keySet(),
                                    new SolrClientRequest
                                            (
                                                    this.solrHome + "-" + this.coreName,
                                                    this.solrHome,
                                                    this.coreName,
                                                    solrUrl,
                                                    this.solrServerMode
                                            )
                            )
                    );
        }
    }

    /*
     * (non-Javadoc)
     * @see com.tridion.storage.si4t.SearchIndex#destroy()
     */
    @Override
    public void destroy() {
        SolrIndexDispatcher.INSTANCE.destroyServers();
    }
}
