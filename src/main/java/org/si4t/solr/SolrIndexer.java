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
import com.tridion.storage.si4t.BaseIndexData;
import com.tridion.storage.si4t.BinaryIndexData;
import com.tridion.storage.si4t.IndexingException;
import com.tridion.storage.si4t.SearchIndex;
import com.tridion.storage.si4t.SearchIndexData;
import com.tridion.storage.si4t.Utils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * SolrIndexer.
 * <p>
 * TODO: Make this completely transactional as well.
 *
 * @author R.S. Kempees
 */
public class SolrIndexer implements SearchIndex {
    private static final Logger LOG = LoggerFactory.getLogger(SolrIndexer.class);

    private String defaultCoreUrl = null;

    private final ConcurrentHashMap<String, BaseIndexData> itemRemovals = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SearchIndexData> itemAdds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BinaryIndexData> binaryAdds = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SearchIndexData> itemUpdates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> solrUrlMap = new ConcurrentHashMap<>();


    private String getSolrUrl(String publicationId) throws ConfigurationException {

        if (solrUrlMap.containsKey(publicationId)) {
            return solrUrlMap.get(publicationId);
        }

        if (!Utils.StringIsNullOrEmpty(this.defaultCoreUrl)) {
            return this.defaultCoreUrl;
        }

        throw new ConfigurationException(
                "Mode is HTTP, but could not find a Url, including a default Url for " + "publication id: " +
                        publicationId);
    }

    /*
     * (non-Javadoc)
     * @see
     * com.tridion.storage.si4t.SearchIndex#configure(com.tridion
     * .configuration.Configuration)
     */

    @Override
    public void configure(Configuration configuration) throws ConfigurationException {
        LOG.debug("Configuration is: " + configuration.toString());
        Configuration indexerConfiguration = configuration.getChild("Indexer");

        String requestMode = indexerConfiguration.getAttribute("Mode");
        if (Utils.StringIsNullOrEmpty(requestMode)) {
            throw new ConfigurationException("Missing Mode attribute. Must either be 'embedded' or 'http'. Exiting");
        }

        if (requestMode.equalsIgnoreCase("http")) {

            if (indexerConfiguration.hasChild("Urls") && indexerConfiguration.getChild("Urls") != null) {
                for (Configuration url : indexerConfiguration.getChild("Urls").getChildren()) {
                    if (url.hasAttribute("Id")) {
                        if (url.getAttribute("Id") != null) {
                            solrUrlMap.put(url.getAttribute("Id"), url.getAttribute("Value"));
                        }
                    }
                }
            }

            if (indexerConfiguration.hasAttribute("DefaultCoreUrl")) {
                this.defaultCoreUrl = indexerConfiguration.getAttribute("DefaultCoreUrl");

                if (!Utils.StringIsNullOrEmpty(this.defaultCoreUrl)) {
                    LOG.info("Setting defaultCoreUrl to: " + this.defaultCoreUrl);
                }
            }

            if ((this.solrUrlMap.isEmpty()) && Utils.StringIsNullOrEmpty(this.defaultCoreUrl)) {
                throw new ConfigurationException(
                        "Request mode is set to HTTP, but no valid Url collection or the DefaultCoreUrl is present. " +
                                "Set the Urls collection in the Indexer configuration node, or add the " +
                                "DefaultCoreUrl to the IndexerConfiguration");
            }

        } else if (requestMode.equalsIgnoreCase("embedded")) {
            throw new ConfigurationException(
                    "Request mode is set to embedded, but this deprecated feature is now removed. Please use the HTTP" +
                            " option.");
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

    /*
     * (non-Javadoc)
     * @see com.tridion.storage.si4t.SearchIndex#commit()
     */
    @Override
    public void commit(String publicationId) throws IndexingException {
        try {

            String solrUrl = getSolrUrl(publicationId);
            LOG.debug("found solr url " + solrUrl + " for publication ID " + publicationId);
            debugLogItems();

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

    private void debugLogItems() {

        if (LOG.isDebugEnabled()) {

            String itemsInItemAdds = itemAdds.entrySet().stream()
                    .map(entry -> "key: " + entry.getKey() + ", val:" + entry.getValue().getUniqueIndexId())
                    .collect(Collectors.joining(","));

            LOG.debug("itemsToAdd contains: {}", itemsInItemAdds);

            String itemsInItemUpdates = itemUpdates.entrySet().stream()
                    .map(entry -> "key: " + entry.getKey() + ", val:" + entry.getValue().getUniqueIndexId())
                    .collect(Collectors.joining(","));

            LOG.debug("itemUpdates contains: {}", itemsInItemUpdates);

            String itemsInBinaryAdds = binaryAdds.entrySet().stream()
                    .map(entry -> "key: " + entry.getKey() + ", val:" + entry.getValue().getUniqueIndexId())
                    .collect(Collectors.joining(","));

            LOG.debug("binaryAdds contains: {}", itemsInBinaryAdds);

            String itemsInItemRemovals = itemRemovals.entrySet().stream()
                    .map(entry -> "key: " + entry.getKey() + ", val:" + entry.getValue().getUniqueIndexId())
                    .collect(Collectors.joining(","));

            LOG.debug("binaryAdds contains: {}", itemsInItemRemovals);

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
     * @throws ParserConfigurationException ParserConfigurationException
     * @throws IOException                  IOException
     * @throws SAXException                 SAXException
     * @throws SolrServerException          SolrServerException
     */
    private void processItemUpdates(String solrUrl)
            throws ParserConfigurationException, IOException, SAXException, SolrServerException {
        this.commitAddContentToSolr(this.itemUpdates, solrUrl);
    }

    private void commitAddBinariesToSolr(String solrUrl)
            throws SolrServerException, IOException, ParserConfigurationException, SAXException {
        if (this.binaryAdds.size() > 0) {
            LOG.info("Adding binaries to Solr.");

            LOG.info(SolrIndexDispatcher.INSTANCE.addBinaries(binaryAdds, new SolrClientRequest(solrUrl)));
        }
    }

    private void commitAddContentToSolr(ConcurrentHashMap<String, SearchIndexData> itemsToAdd, String solrUrl)
            throws SolrServerException, IOException, ParserConfigurationException, SAXException {
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
                groupedDocuments.get(step).add(constructInputDocument(data));
                i++;
            }
            LOG.trace(groupedDocuments.toString());
            this.dispatchAddContentToSolr(groupedDocuments, solrUrl);
        }
    }

    private void dispatchAddContentToSolr(ArrayList<ArrayList<SolrInputDocument>> groupedDocuments, String solrUrl)
            throws ParserConfigurationException, IOException, SAXException, SolrServerException {
        LOG.info("Dispatching documents in " + groupedDocuments.size() + " steps.");

        for (ArrayList<SolrInputDocument> documents : groupedDocuments) {
            if (documents.size() > 0) {
                DispatcherPackage dispatcherPackage =
                        new DispatcherPackage(DispatcherAction.PERSIST, new SolrClientRequest(solrUrl), documents);
                LOG.info(SolrIndexDispatcher.INSTANCE.addDocuments(dispatcherPackage));
            }
        }
    }

    private static SolrInputDocument constructInputDocument(SearchIndexData data) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("pubdate", "NOW");
        LOG.info("Adding Id: " + data.getUniqueIndexId());
        doc.addField("id", data.getUniqueIndexId());

        ConcurrentHashMap<String, ArrayList<Object>> fieldList = data.getIndexFields();

        for (Entry<String, ArrayList<Object>> fieldEntry : fieldList.entrySet()) {
            String fieldName = fieldEntry.getKey();
            for (Object o : fieldEntry.getValue()) {
                doc.addField(fieldName, o);
                LOG.trace("Adding: " + fieldName + ": " + o);
            }
        }
        return doc;
    }

    private void removeItemsFromSolr(String solrUrl)
            throws SolrServerException, IOException, ParserConfigurationException, SAXException {
        if (this.itemRemovals.size() > 0) {
            LOG.info(SolrIndexDispatcher.INSTANCE
                    .removeFromSolr(this.itemRemovals.keySet(), new SolrClientRequest(solrUrl)));
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
