

for selection with inheritance
    
    select ?f where { ?f a %MODEL_ NAME }

for selection without inheritance, use

    select ?f where { ?f a ?class . ?class rdfs:subClassOf* %Class_NAME }
    
To execute the query, call ```sparql(String q)``` the rendered result is an CloesableIterator[Map[String, NODE object]];

Obtain the uri of the node in graph, sample execution in scala console:
    $ res1.next()
    res5: java.util.Map[String,com.mysema.rdfbean.model.NODE] = {f=http://localhost:9000/ontology/BAR/node5995547140467}
    $ res5.get("f")
    res10: com.mysema.rdfbean.model.NODE = http://localhost:9000/BAR/node5995547140467
    $ getById("http://localhost:9000/ontology/BAR/node5995547140467")
    res17: models.BaseNode[_ <: AnyRef] = models.features.activities.BAR@679bac9
    