gmantic
=========
Welcome to the alpha release of gmantic, a semantic graphing framework. 

**Main features:**

  - Object-Ontology mapping via RDFBean
  - Automatic ElasticSearch indexing of text properties & hierarchy-aware search (thanks to useekm ElasticSearchIndexer)
  - Use any Sesame-compatible triplestore
  - Simple API for sparql, text search, etc...

gmantic WILL BE licensed GPLv2. Contact [MeshNet Inc.] regarding commercial licensing.

Getting Started
--------------
If you like SBT, you can do ```compile```, ```test```, ```publish``` as usual.

If you are using sbt, add the following resolver: ```resolvers += "public.opensahara.com" at "http://dev.opensahara.com/nexus/content/groups/public/"``` TODO: stop depending on this repo

If you prefer Maven, do ```clean install```

We will get this bad boy up onto a public repository soon so you can include it as a dependency.

Usage
------------
For now, see the example in src/test/scala/com/meshnet/gmantic/GmanticTest.java.

Ontology exposes a builder pattern for starting up gmantic.

You'll want to extend the Relation or BaseNode classes with your Java objects.

TODO
-----------
* Get RDFBean+QueryDSL working, probably using [play-querydsl](https://github.com/CedricGatay/play-querydsl)
* Apply GPL2 license 

######Copyright MeshNet Inc. 2014.


[MeshNet Inc.]:http://meshnetapp.com
    