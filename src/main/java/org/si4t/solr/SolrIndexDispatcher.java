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

import com.tridion.storage.si4t.BinaryIndexData;
import com.tridion.storage.si4t.Utils;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ContentStreamBase.FileStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * SolrIndexDispatcher.
 * 
 * Singleton. Dispatches updates to the Solr Index.
 * 
 * @author R.S. Kempees
 */
public enum SolrIndexDispatcher
{
	INSTANCE;
	private static ConcurrentHashMap<String, HttpSolrClient> _solrServers = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, HttpClient> _httpClients = new ConcurrentHashMap<>();
	private static final Logger LOG = LoggerFactory.getLogger(SolrIndexDispatcher.class);

	private HttpSolrClient getSolrServer(SolrClientRequest clientRequest) throws SolrServerException {
		switch (clientRequest.getServerMode())
		{
			case EMBEDDED:
				throw new SolrServerException("Embedded Connections are not supported anymore. Change your configuration to use HTTP");

			case HTTP:
			default:
				if (_solrServers.get(clientRequest.getSolrUrl()) == null)
				{
					LOG.info("Obtaining Http Solr server [" + clientRequest.getSolrUrl() + ": " + clientRequest.getSolrUrl());
					this.createHttpSolrServer(clientRequest.getSolrUrl());

				}
				return _solrServers.get(clientRequest.getSolrUrl());

		}
	}

	private void createHttpSolrServer(String url)
	{
		if (_httpClients.get(url) == null)
		{

			final HttpSolrClient solrClient = new HttpSolrClient.Builder(url)
					.withHttpClient(HttpClientBuilder.create().setMaxConnPerRoute(100).build()).build();
			final HttpClient client = solrClient.getHttpClient();

			LOG.debug(">> Creating HttpClient instance");
			_httpClients.put(url, client);
			_solrServers.put(url, solrClient);
		}
		else
		{
			LOG.debug(">> Reusing existing HttpClient instance");

			final HttpSolrClient solrClient = new HttpSolrClient.Builder(url)
					.withHttpClient(_httpClients.get(url)).build();
			_solrServers.put(url, solrClient);
		}
		LOG.info("Created a Commons Http Solr server client instance for " + url);
	}

	public String addBinaries(ConcurrentHashMap<String, BinaryIndexData> binaryAdds, SolrClientRequest clientRequest) throws IOException, SolrServerException {

		HttpSolrClient solrClient;

		solrClient = this.getSolrServer(clientRequest);

		if (solrClient == null)
		{
			throw new SolrServerException("Solr server not instantiated.");
		}
		StringBuilder rsp = new StringBuilder();
		String rspResponse = " path not found";

		for (Map.Entry<String, BinaryIndexData> entry : binaryAdds.entrySet())
		{
			BinaryIndexData data = entry.getValue();

			LOG.debug("Dispatching binary content to Solr.");

			FileStream fs = this.getBinaryInputStream(data);

			String id = data.getUniqueIndexId();
			LOG.info("Indexing binary with Id: " + id + ", and URL Path:" + data.getIndexUrl());
			ContentStreamUpdateRequest up = new ContentStreamUpdateRequest("/update/extract");

			up.addContentStream(fs);

			up.setParam("literal.id", id);
			up.setParam("literal.publicationid",data.getPublicationItemId());
			up.setParam("literal.pubdate", "NOW");
			up.setParam("literal.url", data.getIndexUrl().replace(" ", "%20"));

			if (!Utils.StringIsNullOrEmpty(data.getFileSize()))
            {
                up.setParam("literal.fileSize", data.getFileSize());
            }
			if (!Utils.StringIsNullOrEmpty(data.getFileType()))
            {
                up.setParam("literal.fileType", data.getFileType());
            }
			up.setParam("defaultField", "binary_content");

			UpdateResponse serverrsp;

			serverrsp = up.process(solrClient);
			rsp.append(serverrsp.getResponse());

			LOG.info("Committing adding binaries.");
			rsp.append("\n");

			serverrsp = solrClient.commit();
			rsp.append(serverrsp.getResponse());

			rspResponse = rsp.toString();
		}
		return ("Adding binaries had the following response: " + rspResponse);
	}

	private FileStream getBinaryInputStream(BinaryIndexData data) throws IOException
	{
		String fileName = Utils.GetBinaryFileName(data.getFileName());

		if (fileName.length() < 3)
		{
			fileName += "000";
		}

		String fileExtension = Utils.GetBinaryFileExtension(data.getFileName());
		File tempBinaryFile = null;
		OutputStream out = null;
		try
		{
			tempBinaryFile = File.createTempFile(fileName, fileExtension, null);
			LOG.trace("File created: " + tempBinaryFile.getAbsolutePath());
			tempBinaryFile.deleteOnExit();
			out = new FileOutputStream(tempBinaryFile);
			IOUtils.write(data.getContent().getContent(), out);
			LOG.trace("IOUtils is done writing binary content.");
		} catch (IOException e)
		{
			LOG.error(e.getLocalizedMessage(),e);
			throw e;
		}
		finally
		{
			if ( tempBinaryFile != null ) {
				tempBinaryFile.deleteOnExit();
			}
			IOUtils.closeQuietly(out);
		}

		return new FileStream(tempBinaryFile);
	}

	public String addDocuments(DispatcherPackage dispatcherPackage) throws ParserConfigurationException, IOException, SAXException, SolrServerException {
		HttpSolrClient solrClient = this.getSolrServer(dispatcherPackage.getRequest());
		if (solrClient == null)
		{
			throw new SolrServerException("Solr server not instantiated.");
		}
		ArrayList<SolrInputDocument> documents = dispatcherPackage.getDocuments();

		if (documents == null)
		{
			throw new NullPointerException("Document list is null");
		}

		for (SolrInputDocument d : documents)
		{
			if (d == null || d.isEmpty())
			{
				LOG.error("Document is null Or empty");
			}
			else
			{
				LOG.info(Utils.RemoveLineBreaks(d.toString()));
				solrClient.add(d);
			}
		}

		UpdateResponse serverrsp = solrClient.commit(true, true);

		return ("Processing " + documents.size() + " documents had the following response: " + serverrsp.getResponse());
	}

	public String removeFromSolr(Set<String> ids, SolrClientRequest clientRequest) throws SolrServerException, IOException, ParserConfigurationException, SAXException {
		HttpSolrClient solrClient = this.getSolrServer(clientRequest);
		if (solrClient == null)
		{
			throw new SolrServerException("Solr server not instantiated.");
		}
		ArrayList<String> idList = new ArrayList<>(ids);
		for (String id : idList)
		{
			LOG.debug("Removing: " + id);
		}
		solrClient.deleteById(idList);
		solrClient.optimize(true, true);
		UpdateResponse response = solrClient.commit(true, true);
		return ("Deleting " + ids.size() + " document(s) had the following response: " + response.getResponse());
	}

	public void destroyServers()
	{
		for (Entry<String, HttpClient> clients : _httpClients.entrySet())
		{
			HttpClient client = clients.getValue();
			if (client != null)
			{
				LOG.info("Closing down HttpClient for url: " + clients.getKey());
				client.getConnectionManager().shutdown();
			}
		}
	}

}
