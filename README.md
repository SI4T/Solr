Solr
====

Solr Indexer implementation for use with <a href="http://si4t.github.io">SI4T</a>.

Checkout the <a href="https://github.com/SI4T/Solr/wiki">Project Wiki</a> or <a href="http://si4t.github.io/Solr">Project Website</a> for more information.  

#Web 8 Upgrade Notes
 
- Web 8 needs to add two additional Tridion dependencies in the deployer config:

cd_common_config_legacy.jar
cd_common_util.jar

- The Solr Version needs to be at least 4.8! Read: http://wiki.apache.org/lucene-java/ReleaseNote48#referrer=solr.pl

- Solr4J has greatly minimized dependencies. The only ones needed for Solr 4.8+ are:

  - org.apache.solr:solrj:4.10.2+
  - org.apache.httpcomponents:httpclient:4.3.3
  - commons-io:commons-io:2.4
  - org.apache.httpcomponents.httpcore:4.3
  - org.apache.httpcomponents.httpmime:4.3.1
  - org.apache.zookeeper:zookeeper:3.4.6
  - org.codehaus.woodstox:wstx-asl:3.2.7
  - org.noggit:noggit:0.5
  
  
  
