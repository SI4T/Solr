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

package com.tridion.extensions.storage.search.solr;

/**
 * SolrClientRequest.
 * 
 * @author R.S. Kempees
 * @version 1.20
 * @since 1.00
 */
public class SolrClientRequest
{
	private String searcherId;
	private String solrHome;
	private String solrCore;
	private String solrUrl;
	private ServerMode serverMode;

	public SolrClientRequest(String searcherId, String solrHome, String solrCore, String solrUrl, ServerMode serverMode)
	{
		this.searcherId = searcherId;
		this.solrHome = solrHome;
		this.solrCore = solrCore;
		this.solrUrl = solrUrl;
		this.serverMode = serverMode;
	}

	public String getSearcherId()
	{
		return searcherId;
	}

	public void setSearcherId(String searcherId)
	{
		this.searcherId = searcherId;
	}

	public String getSolrHome()
	{
		return solrHome;
	}

	public void setSolrHome(String solrHome)
	{
		this.solrHome = solrHome;
	}

	public String getSolrCore()
	{
		return solrCore;
	}

	public void setSolrCore(String solrCore)
	{
		this.solrCore = solrCore;
	}

	public String getSolrUrl()
	{
		return solrUrl;
	}

	public void setSolrUrl(String solrUrl)
	{
		this.solrUrl = solrUrl;
	}

	public ServerMode getServerMode()
	{
		return serverMode;
	}

	public void setServerMode(ServerMode serverMode)
	{
		this.serverMode = serverMode;
	}

	public static enum ServerMode
	{
		EMBEDDED,
		HTTP;
	}
}
