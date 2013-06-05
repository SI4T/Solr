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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.si4t.solr.SolrClientRequest.ServerMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.tridion.configuration.Configuration;
import com.tridion.configuration.ConfigurationException;
import com.tridion.storage.si4t.BaseIndexData;
import com.tridion.storage.si4t.BinaryIndexData;
import com.tridion.storage.si4t.IndexingException;
import com.tridion.storage.si4t.SearchIndex;
import com.tridion.storage.si4t.SearchIndexData;
import com.tridion.storage.si4t.Utils;

/**
 * SolrIndexer.
 * 
 * TODO: Make this completely transactional as well.
 * @author R.S. Kempees
 * @version 1.20
 * @since 1.00
 */
public class SolrIndexer implements SearchIndex
{
	private Logger log = LoggerFactory.getLogger(SolrIndexer.class);
	private String solrHome = null;
	private String coreName = null;
	private String solrServerUrl = null;
	private SolrClientRequest.ServerMode solrServerMode = SolrClientRequest.ServerMode.HTTP;
	private List<Configuration> solrCores = null;
	private List<Configuration> solrUrls = null;
	private String defaultCoreUrl = null;
	private ConcurrentHashMap<String, BaseIndexData> itemRemovals = new ConcurrentHashMap<String, BaseIndexData>();
	private ConcurrentHashMap<String, SearchIndexData> itemAdds = new ConcurrentHashMap<String, SearchIndexData>();
	private ConcurrentHashMap<String, BinaryIndexData> binaryAdds = new ConcurrentHashMap<String, BinaryIndexData>();
	private ConcurrentHashMap<String, SearchIndexData> itemUpdates = new ConcurrentHashMap<String, SearchIndexData>();
	private static String INDEXER_NODE = "Indexer";

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
								log.info("Using [" + this.solrServerUrl + "] to connect to.");
								return;
							}
						}
					}
				}
			}
		}
		log.info("No Solr Url found for publication Id: " + pubId + ". Trying to use the DefaultCoreUrl");

		if (!Utils.StringIsNullOrEmpty(this.defaultCoreUrl))
		{
			this.solrServerUrl = this.defaultCoreUrl;
			log.info("Default Url found. Using [" + this.solrServerUrl + "] to connect to.");
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
								log.info("Using [" + this.coreName + "] as Solr core.");

							}
							if (core.getAttribute("SolrHome") != null)
							{
								this.solrHome = core.getAttribute("SolrHome");
								log.info("Using [" + this.solrHome + "] as Solr Home.");
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
		log.debug("Configuration is: " + configuration.toString());
		Configuration indexerConfiguration = configuration.getChild(INDEXER_NODE);

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
					log.info("Setting defaultCoreUrl to: " + defaultCoreUrl);
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
			log.info("Request mode is set to embedded, but is deprecated in future versions.");
			this.solrServerMode = ServerMode.EMBEDDED;
			Configuration cores = indexerConfiguration.getChild("Cores");
			this.solrCores = cores.getChildren();

			if (this.solrCores == null || this.solrCores.size() == 0)
			{
				throw new ConfigurationException("Request mode is set to Embedded, but no valid SolrHome or SolrCore nodelist is present.");
			}
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
			log.error("Addition failed. Unique ID is empty");
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
			log.error("Addition failed. Unique ID is empty");
			return;
		}

		if (data.getFieldSize() == 0)
		{
			log.warn("To be indexed item has no data.");
			log.warn("Item is: " + data.toString());
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
			log.error("Removal addition failed. Unique ID empty");
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
		log.info("Adding removeItemFromIndex: " + data.toString());
		if (Utils.StringIsNullOrEmpty(data.getUniqueIndexId()))
		{
			log.error("Removal addition failed. Unique ID empty");
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
			log.error("Adding update item failed. Unique ID empty");
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
			if (this.solrServerMode == ServerMode.EMBEDDED)
			{
				this.setCoreNameAndSolrHome(publicationId);
			}
			else if (this.solrServerMode == ServerMode.HTTP)
			{
				this.setSolrUrl(publicationId);
			}
			this.commitAddContentToSolr(this.itemAdds);
			this.commitAddBinariesToSolr();
			this.removeItemsFromSolr();
			this.processItemUpdates();
		}

		catch (SolrServerException e)
		{
			logException(e);
			throw new IndexingException("Solr Server Exception: " + e.getMessage());
		}
		catch (IOException e)
		{
			logException(e);
			throw new IndexingException("IO Exception: " + e.getMessage());
		}
		catch (ParserConfigurationException e)
		{
			logException(e);
			throw new IndexingException("ParserConfigurationException: " + e.getMessage());
		}
		catch (SAXException e)
		{
			logException(e);
			throw new IndexingException("SAXException:" + e.getMessage());
		}
		catch (ConfigurationException e)
		{
			logException(e);
			throw new IndexingException("Configuration Exception:" + e.getMessage());
		}
		finally
		{
			log.info("Clearing out registers.");
			this.clearRegisters();
		}
	}

	private void logException(Exception e)
	{
		log.error(e.getMessage());
		log.error(Utils.stacktraceToString(e.getStackTrace()));
	}

	private void clearRegisters()
	{
		itemAdds.clear();
		binaryAdds.clear();
		itemRemovals.clear();
		itemUpdates.clear();
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

	private void commitAddBinariesToSolr() throws SolrServerException, IOException, ParserConfigurationException, SAXException
	{
		if (this.binaryAdds.size() > 0)
		{
			log.info("Adding binaries to Solr.");

			log.info
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

	private void commitAddContentToSolr(ConcurrentHashMap<String, SearchIndexData> itemsToAdd) throws SolrServerException, IOException, ParserConfigurationException, SAXException
	{
		int itemAddsize = itemsToAdd.size();
		if (itemAddsize > 0)
		{
			log.info("Adding pages and component presentations to Solr in batches of 10");

			ArrayList<ArrayList<SolrInputDocument>> groupedDocuments = new ArrayList<ArrayList<SolrInputDocument>>();
			int step = 0;
			int i = 0;

			groupedDocuments.add(new ArrayList<SolrInputDocument>());
			for (Map.Entry<String, SearchIndexData> entry : itemsToAdd.entrySet())
			{
				if (i % 10 == 0 && itemAddsize > 10)
				{
					step++;
					groupedDocuments.add(new ArrayList<SolrInputDocument>());
				}
				SearchIndexData data = entry.getValue();
				groupedDocuments.get(step).add(constructInputDocument(data, log));
				i++;
			}
			log.trace(groupedDocuments.toString());
			this.dispatchAddContentToSolr(groupedDocuments);
		}
	}

	private void dispatchAddContentToSolr(ArrayList<ArrayList<SolrInputDocument>> groupedDocuments) throws ParserConfigurationException, IOException, SAXException, SolrServerException
	{
		log.info("Dispatching documents in " + groupedDocuments.size() + " steps.");

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
				log.info
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

	@SuppressWarnings("unused")
	private static String stripHtmlTags(String input) throws IOException
	{
		StringBuilder out = new StringBuilder();
		StringReader strReader = new StringReader(input);
		HTMLStripCharFilter html = new HTMLStripCharFilter(strReader);
		char[] cbuf = new char[1024 * 10];
		while (true)
		{
			int count = html.read(cbuf);
			if (count == -1)
				break; // end of stream mark is -1
			if (count > 0)
				out.append(cbuf, 0, count);
		}
		html.close();
		return out.toString();
	}

	private void removeItemsFromSolr() throws SolrServerException, IOException, ParserConfigurationException, SAXException
	{
		if (this.itemRemovals.size() > 0)
		{
			log.info
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
