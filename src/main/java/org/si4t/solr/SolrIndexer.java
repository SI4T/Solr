/**
 * Copyright 2011-2013 Radagio & SDL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
 * 
 * TODO: Make this completely transactional as well.
 * @author R.S. Kempees
 */
public class SolrIndexer implements SearchIndex
{
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

	private void setSolrUrl(String pubId) throws ConfigurationException
	{
		if (this.solrUrls != null && !Utils.StringIsNullOrEmpty(pubId))
		{
			for (Configuration url : this.solrUrls)
			{
				if (url.hasAttribute("Id"))
				{
					if (url.getAttribute("Id") != null)
					{
						if (url.getAttribute("Id").equalsIgnoreCase(pubId))
						{
							if (url.getAttribute("Value") != null)
							{
								this.solrServerUrl = url.getAttribute("Value");
								LOG.info("Using [" + this.solrServerUrl + "] to connect to.");
								return;
							}
						}
					}
				}
			}
		}
		LOG.info("No Solr Url found for publication Id: " + pubId + ". Trying to use the DefaultCoreUrl");

		if (!Utils.StringIsNullOrEmpty(this.defaultCoreUrl))
		{
			this.solrServerUrl = this.defaultCoreUrl;
			LOG.info("Default Url found. Using [" + this.solrServerUrl + "] to connect to.");
			return;
		}

		throw new ConfigurationException("Mode is HTTP, but could not find a Url for publication id: " + pubId);
	}

	private void setCoreNameAndSolrHome(String pubId) throws ConfigurationException
	{
		if (this.solrCores != null && !Utils.StringIsNullOrEmpty(pubId))
		{
			for (Configuration core : this.solrCores)
			{
				if (core.hasAttribute("Id"))
				{
					if (core.getAttribute("Id") != null)
					{
						if (core.getAttribute("Id").equalsIgnoreCase(pubId))
						{
							if (core.getAttribute("Name") != null)
							{
								this.coreName = core.getAttribute("Name");
								LOG.info("Using [" + this.coreName + "] as Solr core.");

							}
							if (core.getAttribute("SolrHome") != null)
							{
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
	public void configure(Configuration configuration) throws ConfigurationException
	{
		LOG.debug("Configuration is: " + configuration.toString());
		Configuration indexerConfiguration = configuration.getChild("Indexer");

		String requestMode = indexerConfiguration.getAttribute("Mode");
		if (Utils.StringIsNullOrEmpty(requestMode))
		{
			throw new ConfigurationException("Missing Mode attribute. Must either be 'embedded' or 'http'. Exiting");
		}

		if (requestMode.equalsIgnoreCase("http"))
		{
			this.solrServerMode = ServerMode.HTTP;
			this.solrUrls = indexerConfiguration.getChild("Urls").getChildren();

			String defaultCoreUrl = "";
			if (indexerConfiguration.hasAttribute("DefaultCoreUrl"))
			{
				defaultCoreUrl = indexerConfiguration.getAttribute("DefaultCoreUrl");

				if (!Utils.StringIsNullOrEmpty(defaultCoreUrl))
				{
					LOG.info("Setting defaultCoreUrl to: " + defaultCoreUrl);
					this.defaultCoreUrl = defaultCoreUrl;
				}
			}

			if ((this.solrUrls == null || this.solrUrls.size() == 0) && Utils.StringIsNullOrEmpty(defaultCoreUrl))
			{
				throw new ConfigurationException("Request mode is set to HTTP, but no valid Url collection or the DefaultCoreUrl is present. Set the Urls collection in the Indexer configuration node, or add the DeafultCoreAttribute to the IndexerConfiguration");
			}

		}
		else if (requestMode.equalsIgnoreCase("embedded"))
		{
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
	public void addBinaryToIndex(BinaryIndexData data) throws IndexingException
	{
		if (Utils.StringIsNullOrEmpty(data.getUniqueIndexId()))
		{
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
	public void addItemToIndex(SearchIndexData data) throws IndexingException
	{
		if (Utils.StringIsNullOrEmpty(data.getUniqueIndexId()))
		{
			LOG.error("Addition failed. Unique ID is empty");
			return;
		}

		if (data.getFieldSize() == 0)
		{
			LOG.warn("To be indexed item has no data.");
			LOG.warn("Item is: " + data.toString());
		}

		if (!this.itemAdds.containsKey(data.getUniqueIndexId()))
		{
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
	public void removeBinaryFromIndex(BaseIndexData data) throws IndexingException
	{
		if (Utils.StringIsNullOrEmpty(data.getUniqueIndexId()))
		{
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
	public void removeItemFromIndex(BaseIndexData data) throws IndexingException
	{
		LOG.info("Adding removeItemFromIndex: " + data.toString());
		if (Utils.StringIsNullOrEmpty(data.getUniqueIndexId()))
		{
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
	public void updateItemInIndex(SearchIndexData data) throws IndexingException
	{
		if (Utils.StringIsNullOrEmpty(data.getUniqueIndexId()))
		{
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
	public void commit(String publicationId) throws IndexingException
	{
		try
		{

			this.setSolrUrl(publicationId);

			this.commitAddContentToSolr(this.itemAdds);
			this.commitAddBinariesToSolr();
			this.removeItemsFromSolr();
			this.processItemUpdates();
		}

		catch (SolrServerException e)
		{
			LOG.error(e.getLocalizedMessage(),e);
			throw new IndexingException("Solr Server Exception: " + e.getMessage());
		}
		catch (IOException e)
		{
			LOG.error(e.getLocalizedMessage(),e);
			throw new IndexingException("IO Exception: " + e.getMessage());
		}
		catch (ParserConfigurationException e)
		{
			LOG.error(e.getLocalizedMessage(),e);
			throw new IndexingException("ParserConfigurationException: " + e.getMessage());
		}
		catch (SAXException e)
		{
			LOG.error(e.getLocalizedMessage(),e);
			throw new IndexingException("SAXException:" + e.getMessage());
		}
		catch (ConfigurationException e)
		{
			LOG.error(e.getLocalizedMessage(),e);
			throw new IndexingException("Configuration Exception:" + e.getMessage());
		}
		catch (Throwable e) {
			LOG.error("Unexpected exception: " + e.getLocalizedMessage(),e);
			throw new IndexingException("Unexpected exception:" + e.getMessage());
		}
		finally
		{
			LOG.info("Clearing out registers.");
			this.clearRegisters();
		}
	}

	private void clearRegisters()
	{
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
	 * 
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
	private void processItemUpdates() throws ParserConfigurationException, IOException, SAXException, SolrServerException
	{
		this.commitAddContentToSolr(this.itemUpdates);
	}

	private void commitAddBinariesToSolr() throws SolrServerException, IOException, ParserConfigurationException, SAXException, IndexingException {
		if (this.binaryAdds.size() > 0)
		{
			LOG.info("Adding binaries to Solr.");

			LOG.info
					(
					SolrIndexDispatcher.INSTANCE.
							addBinaries(binaryAdds,
									new SolrClientRequest(
											this.solrHome + "-" + this.coreName,
											this.solrHome,
											this.coreName,
											this.solrServerUrl,
											this.solrServerMode)
							)
					);
		}
	}

	private void commitAddContentToSolr(ConcurrentHashMap<String, SearchIndexData> itemsToAdd) throws SolrServerException, IOException, ParserConfigurationException, SAXException {
		int itemAddsize = itemsToAdd.size();
		if (itemAddsize > 0)
		{
			LOG.info("Adding pages and component presentations to Solr in batches of 10");

			ArrayList<ArrayList<SolrInputDocument>> groupedDocuments = new ArrayList<>();
			int step = 0;
			int i = 0;

			groupedDocuments.add(new ArrayList<>());
			for (Map.Entry<String, SearchIndexData> entry : itemsToAdd.entrySet())
			{
				if (i % 10 == 0 && itemAddsize > 10)
				{
					step++;
					groupedDocuments.add(new ArrayList<>());
				}
				SearchIndexData data = entry.getValue();
				groupedDocuments.get(step).add(constructInputDocument(data, LOG));
				i++;
			}
			LOG.trace(groupedDocuments.toString());
			this.dispatchAddContentToSolr(groupedDocuments);
		}
	}

	private void dispatchAddContentToSolr(ArrayList<ArrayList<SolrInputDocument>> groupedDocuments) throws ParserConfigurationException, IOException, SAXException, SolrServerException {
		LOG.info("Dispatching documents in " + groupedDocuments.size() + " steps.");

		for (ArrayList<SolrInputDocument> documents : groupedDocuments)
		{
			if (documents.size() > 0)
			{
				DispatcherPackage dispatcherPackage = new DispatcherPackage
						(
								DispatcherAction.PERSIST,
								new SolrClientRequest(
										this.solrHome + "-" + this.coreName,
										this.solrHome,
										this.coreName,
										this.solrServerUrl,
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

	private static SolrInputDocument constructInputDocument(SearchIndexData data, Logger log)
	{
		SolrInputDocument doc = new SolrInputDocument();
		doc.addField("pubdate", "NOW");
		log.info("Adding Id: " + data.getUniqueIndexId());
		doc.addField("id", data.getUniqueIndexId());

		ConcurrentHashMap<String, ArrayList<Object>> fieldList = data.getIndexFields();

		for (Entry<String, ArrayList<Object>> fieldEntry : fieldList.entrySet())
		{
			String fieldName = fieldEntry.getKey();
			for (Object o : fieldEntry.getValue())
			{
				doc.addField(fieldName, o);
				log.trace("Adding: " + fieldName + ": " + o);
			}
		}
		return doc;
	}

	private void removeItemsFromSolr() throws SolrServerException, IOException, ParserConfigurationException, SAXException, IndexingException {
		if (this.itemRemovals.size() > 0)
		{
			LOG.info
					(
					SolrIndexDispatcher.INSTANCE.removeFromSolr(
							this.itemRemovals.keySet(),
							new SolrClientRequest
							(
									this.solrHome + "-" + this.coreName,
									this.solrHome,
									this.coreName,
									this.solrServerUrl,
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
	public void destroy()
	{
		SolrIndexDispatcher.INSTANCE.destroyServers();
	}
}
