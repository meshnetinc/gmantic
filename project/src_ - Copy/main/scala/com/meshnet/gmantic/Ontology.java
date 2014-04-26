package com.meshnet.gmantic;

import com.mysema.commons.lang.CloseableIterator;
import com.mysema.rdfbean.model.*;
import com.mysema.rdfbean.model.QueryLanguage;
import com.mysema.rdfbean.model.io.RDFSource;
import com.mysema.rdfbean.object.*;
import com.mysema.rdfbean.sesame.*;
import com.mysema.rdfbean.virtuoso.VirtuosoRepository;
import com.mysema.rdfbean.virtuoso.VirtuosoRepositoryConnection;
import com.useekm.indexing.IndexingSail;
import com.useekm.indexing.IndexingSailConnection;
import com.useekm.indexing.elasticsearch.ElasticSearchIndexer;
import com.useekm.indexing.elasticsearch.ElasticSearchIndexerSettings;
import com.useekm.indexing.elasticsearch.IndexConfiguration;
import com.useekm.indexing.elasticsearch.NodeFactoryBean;
import com.useekm.inference.SimpleTypeInferencingSail;
import com.useekm.reposail.RepositorySail;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.openrdf.Sesame;
import org.openrdf.model.Literal;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import scala.Option;
import scala.Tuple2;

import javax.xml.datatype.XMLGregorianCalendar;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.*;

public class Ontology {

    //public static IndexingRepository smr;
    public static SesameRepository smr;

    public static DefaultConfiguration configuration;
    public static SessionFactoryImpl sessionFactory;
    public static RDFConnection connection;
    public static Class[] mappedClasses;
    public static ArrayList<Long> sparqlQueryTimes = new ArrayList<Long>();
    public static boolean sparqlVerbose = false;
    public static Session session;
    //public static final String NS = "mn:";
    //public static final String NS_FULL = "http://www.semanticweb.org/adam/ontologies/2013/10/untitled-ontology-34#";
    public static final String NS = "http://localhost:9000/ontology/";
    //public static final String NS = "http://www.semanticweb.org/adam/ontologies/2013/10/untitled-ontology-34#";

    public static void initializeRepository(String... sources) {
        RDFSource[] ss = new RDFSource[sources.length];
        int i = 0;
        for (i = 0; i < sources.length; i++) {
            ss[i] = new RDFSource(sources[i], Format.RDFXML, NS);
        }


        initializeRepository(ss);
    }

    //TODO only use synchronized method when absolutely necessary (MemoryStore)
    public synchronized static UID save(BaseNode a) {
        if (session != null) {
            util.Ontology.session.save(a);
            return (UID) Ontology.session.getId(a);
        } else return null;
    }

    public static List<Tuple2<Float, BaseNode<?>>> search(String s, Class... indicies) {
        String[] i = common.getIndiciesJ(indicies);
        SearchResponse r = ((IndexingRepository) smr).getElasticClient().prepareSearch().setIndices(i).setQuery(s).execute().actionGet();
        DateTime prehit = DateTime.now();
        play.Logger.info("Retrieving hits ");

        SearchHit[] ss = r.getHits().getHits();
        Long timetohit = (DateTime.now().getMillis() - prehit.getMillis());
        prehit = DateTime.now();
        Map<Float, BaseNode<?>> b = new HashMap<Float, BaseNode<?>>();
        for (SearchHit sh : ss) {
            play.Logger.info("Getting " + sh.getId() );
            b.put(sh.getScore(), getById(sh.getId()));
        }
        Long timetoretr = (DateTime.now().getMillis() - prehit.getMillis());
        play.Logger.info(timetohit.toString() +","+timetoretr.toString());
        times.add(new Tuple2(timetohit, timetoretr));
        return common.resultsListJ(b);
    }

    public static List<Tuple2<Long,Long>> times = new ArrayList<Tuple2<Long,Long>>();

    public static List<Tuple2<Float, String>> searchIds(String s, Class... indicies) {
        String[] i = common.getIndiciesJ(indicies);
//        play.Logger.info("Searching " + s);
        SearchResponse r = ((IndexingRepository) smr).node.client().prepareSearch().setIndices(i).setQuery(s).execute().actionGet();
//        play.Logger.info("Done search " + s);
        /*SearchResponse response = smr.getElasticClient().prepareSearch("index1", "index2")
                .setTypes("type1", "type2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("multi", "test"))             // Query
              //  .setPostFilter(FilterBuilders.rangeFilter("age").from(12).to(18))   // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();*/

        SearchHit[] ss = r.getHits().getHits();
        Map<Float, String> b = new HashMap<Float,String>();
        for (SearchHit sh : ss) {
//            play.Logger.info("Getting " + sh.getId() );
            b.put(sh.getScore(), (sh.getId()));
        }
        return common.resultsListJ(b);
    }

    



    public static void initializeRepository() {
       // initializeRepository(new RDFSource("classpath:/graph.ttl", Format.TURTLE, NS)); //classpath:/../../base.owl", Format.RDFXML, NS));
       // initializeRepository(new RDFSource("classpath:/base.owl", Format.RDFXML, NS)); //classpath:/../../base.owl", Format.RDFXML, NS));
      //  initializeRepository(); //classpath:/../../base.owl", Format.RDFXML, NS));
        if (Play.application().configuration().getString("initGraph") == null)
            initializeRepository(new RDFSource[0]); //classpath:/../../base.owl", Format.RDFXML, NS));
        else initializeRepository(new RDFSource("classpath:/" + Play.application().configuration().getString("initGraph"), Format.TURTLE, NS));
    }

    public static void configureRepository() {
       String repositoryType = Play.application().configuration().getString("repositoryType");
       if ((repositoryType == null) || (repositoryType.equals("memory"))) {
         smr = new MemoryRepository();
        // smr = new MemoryRepository();
       } else if (repositoryType.equals("elastic-memory")) {
           smr = new IndexingRepository();
       } else if (repositoryType.equals("virtuoso")) {
          /*smr = new VirtuosoRepository(
            Play.application().configuration().getString("virtuosoHost"),
            Play.application().configuration().getString("virtuosoUser"),
            Play.application().configuration().getString("virtuosoPass"),
            Play.application().configuration().getString("virtuosoDefaultContext")
          );*/
       }  else {
           //smr = new MemoryRepository();
       }

    }

    public static void initializeRepository(RDFSource... sources) {

       // smr = new MemoryRepository();
        configureRepository();
        //smr = new NativeRepository(new File("C:\\Users\\Adam\\AppData\\Roaming\\Aduna\\OpenRDF Sesame\\repositories\\meshnet"));
        //smr = new HTTPRepository("http://localhost:8080/openrdf-sesame/repositories/meshnet");
        //smr.setSesameInference(true);
        setSources(sources);
        //shr.setSources(new RDFSource("classpath:/graph.owl", Format.RDFXML, NS));

        sessionFactory = new SessionFactoryImpl();

        /*mappedClasses = new Class[] {
            models.BaseNode.class,
            models.Feature.class,
                Activity.class,
                    models.features.activities.Academic.class, models.features.activities.CareerDevelopment.class, Extracurricular.class, Occupation.class, WorkActivity.class,
                Asset.class,
                    CompletedTask.class, Knowledge.class, Skill.class,
                Association.class,
                    Employer.class, Industry.class,
                Trait.class,
                    Goal.class, Interest.class,
                    PersonalityTrait.class,
                        Ability.class, Preference.class, Temperament.class, Viewpoint.class, WorkStyle.class,
                    PersonalityType.class,
                        MBTI.class,
                    OrganizationValue.class,
                        WorkValue.class,
            Relation.class,
                Importance.class,
                models.relations.Connection.class,
                    Component.class, Effect.class, Implication.class, Predecessor.class, Prerequisite.class, Qualification.class, Aggregate.class, ParentOf.class,
                Description.class, ChainedDescription.class,
            UserNode.class,
        //    Task.class,
            Source.class
        };    */

        configuration = new DefaultConfiguration(
          //  mappedClasses
        );



        configuration.scanPackages(Feature.class.getPackage());



        try {

            smr.initialize();




            sessionFactory.setConfiguration(configuration);
            sessionFactory.setRepository(smr);
            sessionFactory.initialize();

            session = sessionFactory.openSession();



            connection = smr.openConnection();

            setupOntology();

            for (RDFSource s : sources) {
                RepositoryConnection ss = smr.getSesameRepository().getConnection();
                if ((!Play.application().configuration().getBoolean("indexInitial")) && smr instanceof IndexingRepository) {
                    play.Logger.info("Disabling elasticsearch index");
                    ss.add(new StatementImpl(getValueFactory().createURI("cmd:##noindex"), IndexingSail.CMD_NOINDEX, getValueFactory().createLiteral(true)));
                }
                ss.add(s.openStream(), NS, formatConvert(s.getFormat()));
                if ((!Play.application().configuration().getBoolean("indexInitial")) && (Play.application().configuration().getBoolean("index")) && smr instanceof IndexingRepository) {
                    play.Logger.info("Re-enabling elasticsearch index");
                    ss.add(new StatementImpl(getValueFactory().createURI("cmd:##reindex"), IndexingSail.CMD_REINDEX, getValueFactory().createLiteral(true)));
                    ss.commit();
                }
            }

            System.gc();



            //sparql("SELECT ?x WHERE { ?x rdfs:subClassOf " + uri("Trait") + " }");



        } catch (Exception e) {
            e.printStackTrace();
            play.Logger.info(e.getMessage());
            for (StackTraceElement ste : e.getStackTrace()) play.Logger.info(ste.toString());
        }


    }

    public static List<Tuple2<Float, Feature>> searchNames(String title, Class... cls) {
        //List<Tuple2<Float, BaseNode<?>>> pre = util.Ontology.search("{ \"match_phrase\": { \"altName\":\"" + title + "\" }, \"match_phrase\": { \"displayName\":\"" + title + "\" } }", Occupation.class);

        List<Tuple2<Float, BaseNode<?>>> pre = util.Ontology.search("{ \"fuzzy_like_this\": { \"fields\":[\"displayName\",\"altName\"], \"like_text\": \"" + title + "\" } }", cls);
        List<Tuple2<Float, Feature>> res = new ArrayList<Tuple2<Float, Feature>>();
        for (Tuple2<Float, BaseNode<?>> tup : pre) {
            if (tup._2() instanceof Feature) {
                //Logger.info("For " + title + " found similar: " + tup._2().displayName);
                res.add(new Tuple2<Float, Feature>(tup._1(), (Feature) tup._2()));
            }
        }
        return res;
    }

    public static RDFFormat formatConvert(Format f) {
        if (f.equals(Format.N3)) return RDFFormat.N3;
        else if (f.equals(Format.NTRIPLES)) return RDFFormat.NTRIPLES;
        else if (f.equals(Format.RDFXML)) return RDFFormat.RDFXML;
        else if (f.equals(Format.TRIG)) return RDFFormat.TRIG;
        else if (f.equals(Format.TURTLE)) return RDFFormat.TURTLE;
        else return RDFFormat.RDFXML;
    }

    public static void setupOntology() {

        for (MappedClass mc : configuration.getMappedClasses()) {
            //String s = mc.getJavaClass().getSimpleName() + ": ";
            for (MappedClass msc : mc.getMappedSuperClasses()) {
              //  s += " " + msc.getJavaClass().getSimpleName();
                addTripleBase(new UID(NS, mc.getJavaClass().getSimpleName()), RDFS.subClassOf, new UID(NS, msc.getJavaClass().getSimpleName()));

            }
            addTripleBase(new UID(NS, mc.getJavaClass().getSimpleName()), RDFS.subClassOf, new UID(NS, mc.getJavaClass().getSimpleName()));
            //play.Logger.info(s);
        }
    }

    public static void setSources(RDFSource... r) {
        if (smr instanceof SesameRepository) {
            ((SesameRepository) smr).setSources(r);
        } /* else if (smr instanceof VirtuosoRepository) {
            ((VirtuosoRepository) smr).setSources(r);
        } */


    }

    public static void addTriple(UID s, UID p, UID o) {
        addTriple(s,p,o, getRepCon());
    }

    public static void addTripleBase(UID s, UID p, UID o) {
        try {
            if (smr instanceof IndexingRepository)
                addTriple(s,p,o, ((IndexingRepository) smr).baseRepo.getConnection());
            else addTriple(s,p,o, getRepCon());
        } catch (Exception e) {
            play.Logger.error(e.getMessage());
        }
    }

    public static void addTriple(UID s, UID p, UID o,  RepositoryConnection sc) {
        if (smr instanceof SesameRepository) {
            ValueFactory vf = getValueFactory();
            //RepositoryConnection sc = getRepCon();
            try {
                sc.add(
                  vf.createStatement(vf.createURI(s.getNamespace(), s.getLocalName()), vf.createURI(p.getNamespace(), p.getLocalName()), vf.createURI(o.getNamespace(), o.getLocalName()))
                );
            } catch (org.openrdf.repository.RepositoryException re) {
                throw new RepositoryException(re);
            }
        } /* else if (smr instanceof VirtuosoRepository) {
           VirtuosoRepository vr = (VirtuosoRepository) smr;
           ArrayList<STMT> smts = new ArrayList<STMT>();
           smts.add(new STMT(s,p,o));
           try {
               VirtuosoRepositoryConnection vrc = vr.openConnection();
               vrc.addBulk(smts);
               vrc.close();
           } catch (Exception e) {
               throw new RepositoryException(e);
           }
        } else throw new RepositoryException("Unsupported repository type"); */
    }

    public static void addTriple(UID s, UID p, Object l) {
        if (smr instanceof SesameRepository) {
            ValueFactory vf = getValueFactory();
            vf.createLiteral(true);
            RepositoryConnection sc = getRepCon();
            try {
                Literal ll = null;
                if (l instanceof String) ll = vf.createLiteral((String) l);
                if (l instanceof Boolean) ll = vf.createLiteral((boolean) l);
                if (l instanceof Byte) ll = vf.createLiteral((byte) l);
                if (l instanceof Short) ll = vf.createLiteral((short) l);
                if (l instanceof Integer) ll = vf.createLiteral((int) l);
                if (l instanceof Long) ll = vf.createLiteral((long) l);
                if (l instanceof Float) ll = vf.createLiteral((float) l);
                if (l instanceof Double) ll = vf.createLiteral((double) l);
                if (l instanceof XMLGregorianCalendar) ll = vf.createLiteral((XMLGregorianCalendar) l);
                //if (l instanceof DateTime) ll = vf.createLiteral((DateTime) l);


                sc.add(
                  vf.createStatement(vf.createURI(s.getNamespace(), s.getLocalName()), vf.createURI(p.getNamespace(), p.getLocalName()), ll)
                );
            } catch (org.openrdf.repository.RepositoryException re) {
                throw new RepositoryException(re);
            }
        } /*else if (smr instanceof VirtuosoRepository) {
           VirtuosoRepository vr = (VirtuosoRepository) smr;
           ArrayList<STMT> smts = new ArrayList<STMT>();
           smts.add(new STMT(s,p,new LIT(l.toString())));
           try {
               VirtuosoRepositoryConnection vrc = vr.openConnection();
               vrc.addBulk(smts);
               vrc.close();
           } catch (Exception e) {
               throw new RepositoryException(e);
           }
        } else throw new RepositoryException("Unsupported repository type"); */
    }

    public static void test() {
     //   initializeRepository();
        sparqlVerbose = false;
        //service.OnetImportService.importContentModel();
        sparqlVerbose = true;
       /* Ability a = new Ability();
        a.setDisplayName("Same function");
        Ability b = new Ability();
        b.setDisplayName("Same other function");
        a.synonyms = new ArrayList<Feature>();
        a.synonyms.add(b);
        a.prospective = true;
        Session s = sessionFactory.openSession();
        s.save(a);
        s.save(b);
        s.flush();
        s.clear();     */
      //  return new RDFCreator(a).addedStatements;
    }

    public static <T extends BaseNode> T getById(String id, Class<T> c) {
        Session s = session;
        T t = null;

        try {
            return s.get(c, new UID(NS, id));
        } catch (Throwable th) {
          //  System.out.println("Getting by id " + id + " a " + c.getName());
            return c.cast(getById(id));
        }
    }

    public static BaseNode getById(URI u) {
        return getById(new UID(u.toString()));
    }

    public static BaseNode getById(String id) {
        String uri = id;
        if (!id.contains(NS)) uri = NS + id;
       // System.out.println("Getting by id uri " + new UID(uri));
        return getById(new UID(uri));
    }

    public static BaseNode getById(UID u) {
        Session s = session;
        return session.get(BaseNode.class, u);
        /*CloseableIterator<Map<String,NODE>> m = sparql("select ?parents where { <" + u.getNamespace() + u.getLocalName() + "> rdf:type ?class . ?class rdfs:subClassOf* ?parents }");
        String className = "Feature";

        MappedClass clz = findClass(className);
        while (m.hasNext()) {
            Map<String,NODE> r = m.next();

         //   System.out.println("1");
            if (!r.isEmpty()) {
                NODE mc = r.get("parents");
           //     System.out.println("2");
                if (mc.isURI()) {
                    className = mc.asURI().getLocalName();
                    clz = findClass(className);
                    if (clz == null) continue;
                //    System.out.println("3");
                    BaseNode t = null;
                    try {
                      t = (BaseNode) (s.get(clz.getJavaClass(), u)); //getById(id, (Class<T>) clz.getJavaClass().newInstance().getClass());
                    } catch (Throwable th) {
                        th.printStackTrace();
                        continue;
                    }
                    if (t != null) return t;
                }
            }
        }


        play.Logger.info(u.toString() + " not found");
        return null;      */
    }

    public static <T extends BaseNode> T getById(ID id, Class<T> c) {
        Session s = session;
        T t = null;
        try {
            t = s.get(c, id);
        } catch (Throwable h) {
            t = c.cast(getById(id.asURI().getLocalName()));
        }
        return t;
    }

    public static ValueFactory getValueFactory() {
        try {
            return ((SesameRepository) smr).getSesameRepository().getConnection().getValueFactory();
        } catch (Throwable t) {
            return null;
        }
    }

    public static RepositoryConnection getRepCon() {
        try {
            return ((SesameRepository) smr).getSesameRepository().getConnection();
        } catch (Throwable t) {
            return null;
        }
    }

    public static <T extends BaseNode> Class<T> getClass(String c) {
        for (MappedClass m : configuration.getMappedClasses()) {

        }
        return null;
    }

    public static <T extends BaseNode> List<T> findInstances(Class<T> c) {
        String classUrl =  configuration.getMappedClass(c).getUID().toString();
        CloseableIterator<Map<String,NODE>> res = sparql("select ?x where { ?x rdf:type ?y . ?y rdfs:subClassOf* <" + classUrl + "> }");
        ArrayList<T> ret = new ArrayList<T>();
        while (res.hasNext()) {
            for (NODE n : res.next().values()) {
               ret.add(getById(n.asResource(), c));
            }
        }
        return ret;
    }

    public static <T extends BaseNode> List<T> sparqlFind(String qry, String idVar, Class<T> c) {
        Long l = System.nanoTime();
        CloseableIterator<Map<String,NODE>> res = sparql(qry);
        l = System.nanoTime() - l;
        sparqlQueryTimes.add(l);
        ArrayList<T> result = new ArrayList<T>();
   //     System.out.println("between here");
        while (res.hasNext()) {
            for (Map.Entry<String, NODE> n : res.next().entrySet()) {
                if (n.getKey().equals(idVar.replace("?",""))) {
                    if (n.getValue().isResource()) {

                       result.add(getById(n.getValue().asResource(), c));
                        break;
                    }
                }
            }
        }
        if (result.size() > 1) System.out.println(result.size());
        //System.out.println("And here?");
        return result;
    }

    public static <T extends BaseNode> T sparqlFindFirst(String qry, String idVar, Class<T> c) {
        CloseableIterator<Map<String,NODE>> res = sparql(qry);

        if (res.hasNext()) {
            for (Map.Entry<String, NODE> n : res.next().entrySet()) {
                if (n.getKey().equals(idVar.replace("?",""))) {
                    if (n.getValue().isResource()) {
                        return (getById(n.getValue().asResource(), c));
                    }
                }
            }
        }
        return null;
    }



    public static  List<BaseNode> sparqlFind(String qry, String idVar) {
        CloseableIterator<Map<String,NODE>> res = sparql(qry);
        ArrayList<BaseNode> result = new ArrayList<BaseNode>();
        while (res.hasNext()) {
            for (Map.Entry<String, NODE> n : res.next().entrySet()) {
                if (n.getKey().equals(idVar.replace("?",""))) {
                    if (n.getValue().isResource()) {
                        NODE nn = n.getValue();
                        if (nn.isURI()) result.add(getById(nn.asURI().getLocalName()));
                    }
                }
            }
        }
        return result;
    }
   

    public static CloseableIterator<Map<String,NODE>> sparql(String q) {
      return sparql(common.percentsToURIs(q), QueryLanguage.TUPLE, org.openrdf.query.QueryLanguage.SPARQL);

    }

    /*public static CloseableIterator<Map<String,NODE>> sparqlw(String... q) {
       return sparql(sparqlWherePrepare("", q));
    }*/

    public static MappedClass findClass(String f) {
        Option<MappedClass> res = common.findClass(f);
        if (res.isDefined()) return res.get();
        else return null;
    }

    private static String sparqlWherePrepare(String nodeVar, String... q) {
        String s = "SELECT * WHERE { ";
        if (!nodeVar.equals(""))  s = "SELECT " + nodeVar + " WHERE { ";
        Integer ugly = 0;
        if (q.length == 0) ugly = 1 / 0;
        for (String qq : q) {
            s += qq + " . ";
        }
        s = s.substring(0, s.length() - 2) + " }";
        return s;
    }

    public static <T extends BaseNode> List<T> sparqlw(String nodeVar, Class<T> cls, String... q) {
        return sparqlFind(sparqlWherePrepare(nodeVar, q), nodeVar, cls);
    }

    public static List<BaseNode> sparqlw(String nodeVar, String... q) {
        return sparqlFind(sparqlWherePrepare(nodeVar,q), nodeVar);
    }

    public static CloseableIterator<Map<String,NODE>> sparql(String q, QueryLanguage queryType, org.openrdf.query.QueryLanguage language) {
        String f = "prefix :    <" + NS + "> \n";
        f += "prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n";
        f += "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n";
        f += "prefix owl: <http://www.w3.org/2002/07/owl#> \n";
        f += "prefix fts: <http://rdf.useekm.com/fts#> \n";
        f += q;
        if (sparqlVerbose) System.out.println(f);

        RDFConnection conn = connection;
        //Long l = System.nanoTime();
        CloseableIterator<Map<String,NODE>> rows = null;
        try {

           rows = smr.openConnection().createQuery(QueryLanguage.SPARQL, f).getTuples();

           /*RepositoryConnection smrC = smr.getSesameRepository().getConnection();

           SesameDialect d = (SesameDialect) ((SesameConnection) conn).getDialect();
           if (queryType == QueryLanguage.GRAPH){
               Query query = smrC.prepareGraphQuery(language, q);
               rows = new GraphQueryImpl((GraphQuery)query, d ).getTuples();
           } else if (queryType == QueryLanguage.TUPLE){
               Query query = smrC.prepareTupleQuery(language, q);
               rows = new TupleQueryImpl((TupleQuery)query, d ).getTuples();

           } else if (queryType == QueryLanguage.BOOLEAN){
               Query query = smrC.prepareBooleanQuery(language, q);
               rows = new BooleanQueryImpl((BooleanQuery)query, d ).getTuples();
           } else{
               throw new RepositoryException("Unsupported query type " + queryType.getClass().getName());
           }

           /*while (rows.hasNext()) {
               Map<String,NODE> row = rows.next();

               play.Logger.of(Ontology.class).info(row.toString());

               System.out.println(row.toString());
           } */
       } catch (Exception e) {
            System.out.println("Broken by: " + f);
            System.out.println(e.getMessage());
            e.printStackTrace();
       }
        return rows;
    }

    /*public static void sparqlStream(String q, Writer writer, String contentType, QueryLanguage queryType, org.openrdf.query.QueryLanguage language) {

        if (sparqlVerbose) System.out.println(q);

        RDFConnection conn = connection;
        CloseableIterator<Map<String,NODE>> rows = null;
        try {
            RepositoryConnection smrC = smr.getSesameRepository().getConnection();
            smr.openConnection().createQuery(QueryLanguage.SPARQL, q).streamTriples(writer, contentType);
            /*SesameDialect d = (SesameDialect) ((SesameConnection) conn).getDialect();
            if (queryType == QueryLanguage.GRAPH){
                Query query = smrC.prepareGraphQuery(language, q);
                new GraphQueryImpl((GraphQuery)query, d ).streamTriples(writer, contentType);
            } else if (queryType == QueryLanguage.TUPLE){
                Query query = smrC.prepareTupleQuery(language, q);
                new TupleQueryImpl((TupleQuery)query, d ).streamTriples(writer, contentType);
            } else if (queryType == QueryLanguage.BOOLEAN){
                Query query = smrC.prepareBooleanQuery(language, q);
                new BooleanQueryImpl((BooleanQuery)query, d ).streamTriples(writer, contentType);
            } else{
                throw new RepositoryException("Unsupported query type " + queryType.getClass().getName());
            }

           /*while (rows.hasNext()) {
               Map<String,NODE> row = rows.next();

               play.Logger.of(Ontology.class).info(row.toString());

               System.out.println(row.toString());
           }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }    */

    public static void export(String fn) {
        try {
            FileOutputStream fos = new FileOutputStream(new File(fn));
            smr.export(Format.RDFXML, null, fos);
            fos.close();
        } catch (Exception e) {
           e.printStackTrace();
        }

    }

    public static void load(String fn) {
        try {
            FileInputStream fos = new FileInputStream(new File(fn));
            smr.load(Format.RDFXML, fos, null, false);
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static void exportTtl(String fn) {
        try {
            FileOutputStream fos = new FileOutputStream(new File(fn));
            smr.export(Format.TURTLE, null, fos);
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    public static Client getElasticClient() {
        return ((IndexingRepository) smr).getElasticClient();
    }

    public static ElasticSearchIndexerSettings elasticSettings() {

        return ((IndexingRepository) smr).esi.getSettings();
    }

    public static void commit() {
        try {
            smr.getSesameRepository().getConnection().commit();
            ((IndexingRepository) smr).esi.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void loadTtl(String fn) {

        try {
            FileInputStream fos = new FileInputStream(new File(fn));
            smr.load(Format.TURTLE, fos, null, false);
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }







    public static String uri(String r) {
        return "%" + r;
    }




}
