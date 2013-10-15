twitter-cep-web
============

Drools Fusion + Twitter4J + Rest + Guvnor demo

* Setup

- Download JBoss AS 7.0.2 (http://www.jboss.org/jbossas/downloads/)
- Download Guvnor 5.5.0.Final (http://www.jboss.org/drools/downloads)
- Copy src/main/resources/twitter4j.properties.orig to src/main/resources/twitter4j.properties
- Go to https://dev.twitter.com/ and register your application to acquire consumerKey/consumerSecret/accessToken/accessTokenSecret
- Fill twitter4j.properties with those values
- mvn clean install
- Start JBoss AS
- Deploy guvnor-5.5.0.Final-jboss-as-7.0.war to JBoss AS
- Go http://localhost:8080/guvnor-5.5.0.Final-jboss-as-7.0/
- Import repository_export_twittercep.xml
- Deploy target/twitter-cep-web.war to JBoss AS
- Go http://localhost:8080/twitter-cep-web/

* Enjoy!

- If you click "Start online", you will see tweets which match rules in Guvnor (by default, "Dump tweets from people laughing")
- Go to Guvnor and modify the package. For example, disable "Dump tweets from people laughing", enable "Dump tweets from US" and build the package.
- you will see that the rule change is detected by KnowledgeAgent and results will be changed without "Stop/Start"

- If you click "Start offline", it will read tweets from src/main/resources/twitterstream.dump which was previously persisted. You can re-dump tweets by TwitterDumper.

- Now I set twitter4j version to 3.0.3 in pom.xml. If you change it to higher version, you might need to upload twitter4j-core-x.x.x.jar as a model jar 'twitter4j' in Guvnor.
