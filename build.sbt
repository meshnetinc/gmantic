name := "gmantic"

version := "0.1"

scalaVersion := "2.11.0"

mappings in (Compile,packageBin) ~= { (ms: Seq[(File, String)]) =>
      ms filter { case (file, toPath) =>
        toPath != "com/useekm/indexing/elasticsearch/ElasticSearchIndexerSettings$1.class" &&
        toPath != "com/useekm/indexing/elasticsearch/ElasticSearchIndexerSettings$EsMapping.class" &&
        toPath != "com/useekm/indexing/elasticsearch/ElasticSearchIndexerSettings.class"
      }
    }
	
resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.play" % "play-json_2.10" % "2.2.1"

libraryDependencies += "com.novocode" % "junit-interface" % "0.10" % "test"

libraryDependencies += "commons-configuration" % "commons-configuration" % "1.7"
	
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.7"
	
libraryDependencies += "com.mysema.rdf" % "rdfbean-core" % "1.6.1"

libraryDependencies += "com.mysema.rdf" % "rdfbean-sesame2" % "1.6.1"

libraryDependencies += "org.springframework" % "spring-core" % "4.0.3.RELEASE"

libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % "2.3.1"

libraryDependencies += "org.springframework" % "spring-beans" % "4.0.3.RELEASE"

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.7"

libraryDependencies += "com.mysema.rdf" % "rdfbean-rdfs" % "0.7.5"

libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % "2.3.1"

libraryDependencies += "com.mysema.rdf" % "rdfbean-owl" % "0.7.5"

libraryDependencies += "com.mysema.rdf" % "rdfbean-sparql" % "1.6.1"

libraryDependencies += "org.openrdf.sesame" % "sesame-queryparser-sparql" % "2.6.7"

libraryDependencies += "org.openrdf.sesame" % "sesame-repository-http" % "2.6.7"

libraryDependencies += "org.openrdf.sesame" % "sesame-repository-sail" % "2.6.7"

libraryDependencies += "org.openrdf.sesame" % "sesame-rio-ntriples" % "2.6.7"

libraryDependencies += "org.openrdf.sesame" % "sesame-rio-rdfxml" % "2.6.7"

libraryDependencies += "org.openrdf.sesame" % "sesame-rio-turtle" % "2.6.7"

libraryDependencies += "org.openrdf.sesame" % "sesame-sail-memory" % "2.6.7"

libraryDependencies += "org.openrdf.sesame" % "sesame-sail-nativerdf" % "2.6.10"   //was 2.6.7

libraryDependencies += "com.mysema.querydsl" % "querydsl-core" % "3.2.4"

libraryDependencies += "com.mysema.querydsl" % "querydsl-codegen" % "3.2.4"

libraryDependencies += "com.mysema.querydsl" % "querydsl-apt" % "3.2.4"

libraryDependencies += "com.mysema.querydsl" % "querydsl-scala" % "3.2.4"

libraryDependencies += ("com.opensahara" % "useekm-elasticsearch" % "1.2.2-SNAPSHOT").excludeAll(ExclusionRule(organization="org.openrdf.sesame"))

resolvers += "public.opensahara.com" at "http://dev.opensahara.com/nexus/content/groups/public/"

resolvers += "aduna" at "http://maven.ontotext.com/content/repositories/aduna/"