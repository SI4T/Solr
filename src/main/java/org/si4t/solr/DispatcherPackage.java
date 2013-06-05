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

import java.util.ArrayList;

import org.apache.solr.common.SolrInputDocument;

/**
 * DispatcherPackage.
 * 
 * @author R.S. Kempees
 * @version 1.20
 * @since 1.00
 */
public class DispatcherPackage
{
	private DispatcherAction action;
	private SolrClientRequest request;
	private ArrayList<SolrInputDocument> documents;

	public DispatcherPackage(DispatcherAction action, SolrClientRequest request, ArrayList<SolrInputDocument> documents)
	{
		super();
		this.action = action;
		this.request = request;
		this.documents = documents;
	}

	public DispatcherAction getAction()
	{
		return action;
	}

	public void setAction(DispatcherAction action)
	{
		this.action = action;
	}

	public SolrClientRequest getRequest()
	{
		return request;
	}

	public void setRequest(SolrClientRequest request)
	{
		this.request = request;
	}

	public ArrayList<SolrInputDocument> getDocuments()
	{
		return documents;
	}

	public void setDocuments(ArrayList<SolrInputDocument> documents)
	{
		this.documents = documents;
	}
}
