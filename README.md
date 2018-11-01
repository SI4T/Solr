Solr
====

Solr Indexer implementation for use with <a href="http://si4t.github.io">SI4T</a>.

Checkout the <a href="https://github.com/SI4T/Solr/wiki">Project Wiki</a> or <a href="http://si4t.github.io/Solr">Project Website</a> for more information.  

#Web 8 Upgrade Notes
 
- Web 8 needs to add two additional Tridion dependencies in the deployer config:

cd_common_config_legacy.jar
cd_common_util.jar

- The Solr Version needs to be at least 7.5! Read: https://lucene.apache.org/solr/guide/7_5/solr-upgrade-notes.html

- Solr4J has the following dependencies. The ones needed for Solr 7.5+ are:

  - org.apache.solr:solrj:4.10.2+
  - org.apache.httpcomponents:httpclient:4.5.3+
  - commons-io:commons-io:2.4+
  - org.apache.httpcomponents.httpcore:4.4.6+
  - org.apache.httpcomponents.httpmime:4.5.3+
  - org.apache.zookeeper:zookeeper:3.4.11+
  - org.codehaus.woodstox:stax2-api:3.1.4+
  - org.codehaus.woodstox:woodstox-core-asl:4.4.1
  - org.noggit:noggit:0.8+
  
  
  
