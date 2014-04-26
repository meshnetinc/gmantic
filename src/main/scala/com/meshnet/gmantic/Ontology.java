package com.meshnet.gmantic;

import com.mysema.commons.lang.CloseableIterator;
import com.mysema.rdfbean.annotations.ClassMapping;
import com.mysema.rdfbean.model.*;
import com.mysema.rdfbean.model.QueryLanguage;
import com.mysema.rdfbean.model.RepositoryException;
import com.mysema.rdfbean.model.io.RDFSource;
import com.mysema.rdfbean.object.*;
import com.mysema.rdfbean.sesame.*;
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
import org.openrdf.Sesame;
import org.openrdf.model.Literal;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.repository.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;
import scala.Option;
import scala.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.*;

public class Ontology {

    private SesameRepository smr;
	final static Logger logger = LoggerFactory.getLogger(Ontology.class);
    private DefaultConfiguration configuration;
    private SessionFactoryImpl sessionFactory;
    private RDFConnection connection;
    private boolean sparqlVerbose = false;
    private Session session;
    private String NS = "http://graph.local/ontology/";
    private static Ontology o;

	public static String getNamespace() {
		return o.NS;
	}
	
	protected void setNamespace(String namespace) {
        updateClassMapping(namespace, BaseNode.class);
        updateClassMapping(namespace, Relation.class);
		NS = namespace;
	}

    private void updateClassMapping(String nns, Class node) {
        final ClassMapping cm = (ClassMapping) node.getAnnotations()[0];
        final String ns = nns;
        final String ln = node.getSimpleName();
        Annotation newAnn = new ClassMapping() {

            @Override
            public String ln() {
                return ln;
            }

            @Override
            public String ns() {
                return ns;
            }

            @Override
            public Class<?> parent() {
                return null;
            }

            @Override
            public Class<? extends Annotation> annotationType() {
                return ClassMapping.class;
            }
        };
        try {
            Field field = Class.class.getDeclaredField("annotations");
            field.setAccessible(true);
            Map<Class<? extends Annotation>, Annotation> annotations = (Map<Class<? extends Annotation>, Annotation>) field.get(node);

            annotations.put(ClassMapping.class, newAnn);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            for (StackTraceElement ee : e.getStackTrace()) logger.error(ee.toString());
        }
    }

    //TODO only use synchronized method when absolutely necessary (MemoryStore)
    protected synchronized UID _save(BaseNode a) {
        if (session != null) {
            session.save(a);
            return (UID) session.getId(a);
        } else return null;
    }

    public static synchronized UID save(BaseNode a) {
        return o._save(a);
    }

	private Ontology() {}
	
	public static OntologyBuilder config(String ns) {
		return new OntologyBuilder(ns);
	}

    public static boolean ready() {
        return o != null;
    }
	
	static class BaseOntologyBuilder {
		protected String namespace = "http://graph.local/ontology/";
		protected RDFSource[] sources = null;
		protected boolean elastic = true;
		protected File elasticConf = null;
		protected ElasticSearchIndexerSettings elasticSettings = null;
		protected boolean defaultIndexConfigs = true;
		protected Long initTimeout = 30000L;
		protected Class[] classesToMap;
		protected Package[] packagesToMap;
		protected Sail sr;
		protected boolean indexInitialSourcesOnBoot = false;
		protected boolean indexNewData = true;
		
		public BaseOntologyBuilder sources(RDFSource... sources) {
			this.sources = sources;
			return this;
		}
		
		public BaseOntologyBuilder classes(Class... classesToMap) {
			this.classesToMap = classesToMap;
			return this;
		}
		
		public BaseOntologyBuilder packages(Package... packagesToMap) {
			this.packagesToMap = packagesToMap;
			return this;
		}
		public void start() {
			if ((elasticConf == null) || (sr != null)) {
				elastic = false;
			}

			Ontology o = new Ontology();
			o.setNamespace(namespace);


            ArrayList<Class> clses = new ArrayList<Class>();
            clses.add(BaseNode.class);
            clses.add(Relation.class);
            if (classesToMap != null)
                clses.addAll(Arrays.asList(classesToMap));
            o.configuration = new DefaultConfiguration(clses.toArray(new Class[0]));


            if (packagesToMap != null)
                o.configuration.scanPackages(packagesToMap);

            if ((sr != null) && !elastic)
                o.smr = new SesameRepository() {
                    long i = 0;
                    @Override
                    protected Repository createRepository(boolean sesameInference) {
                        return new SailRepository(sr);
                    }

                    @Override
                    public long getNextLocalId() {
                        i ++;
                        return i;
                    }
                };
            else {
                if ((sr != null) && elastic) o.smr = new IndexingRepository(namespace, initTimeout, elasticConf, elasticSettings, defaultIndexConfigs, o.configuration.getMappedClasses().toArray(new MappedClass[0]), sr);
                else if ((sr == null) && elastic) o.smr = new IndexingRepository(namespace, initTimeout, elasticConf, elasticSettings, defaultIndexConfigs, o.configuration.getMappedClasses().toArray(new MappedClass[0]));
                else o.smr = new MemoryRepository();
            }
			o.initializeRepository(indexInitialSourcesOnBoot, indexNewData, classesToMap, packagesToMap, sources);
            setOntology(o);
		}
	}

    protected static void setOntology(Ontology oo) {
        if (o == null) o = oo;
    }

	static class OntologyBuilder extends BaseOntologyBuilder {
		protected OntologyBuilder(String namespace) {
			this.namespace = namespace;
		}
		public void start(Sail sr) {
			this.sr = sr;
			start();
		}
        public OntologyBuilder sources(RDFSource... sources) {
            this.sources = sources;
            return this;
        }

        public OntologyBuilder classes(Class... classesToMap) {
            this.classesToMap = classesToMap;
            return this;
        }

        public OntologyBuilder packages(Package... packagesToMap) {
            this.packagesToMap = packagesToMap;
            return this;
        }
		public ElasticOntologyBuilder elastic(File elasticConf) {
			elastic = true;
			this.elasticConf = elasticConf;
			return new ElasticOntologyBuilder(this);
		}
	}
	static class ElasticOntologyBuilder extends BaseOntologyBuilder {
		protected ElasticOntologyBuilder(BaseOntologyBuilder b) {
			this.namespace = b.namespace;
			this.sources = b.sources;
			this.elastic = true;
			this.elasticConf = b.elasticConf;
			this.elasticSettings = b.elasticSettings;
			this.defaultIndexConfigs = b.defaultIndexConfigs;
			this.packagesToMap = b.packagesToMap;
			this.classesToMap = b.classesToMap;
			this.sr = b.sr;
		}
		public ElasticOntologyBuilder indexerSettings(ElasticSearchIndexerSettings elasticSettings) {
			this.elasticSettings = elasticSettings;
			return this;
		}
        public ElasticOntologyBuilder initialTimeout(Long timeout) {
            this.initTimeout = timeout;
            return this;
        }
		public ElasticOntologyBuilder indexInitialSourcesOnBoot (boolean indexInitialSourcesOnBoot) {
			this.indexInitialSourcesOnBoot = indexInitialSourcesOnBoot;
			return this;
		}
		public ElasticOntologyBuilder indexNewData (boolean indexNewData) {
			this.indexNewData = indexNewData;
			return this;
		}
		
		public ElasticOntologyBuilder defaultIndexConfigs(boolean useDefaults) {
			defaultIndexConfigs = useDefaults;
			return this;
		}
	}

    public static List<Tuple2<Float, BaseNode<?>>> search(String s, Class... indicies) {
        return o._search(s,indicies);
    }

    public static List<Tuple2<Float, BaseNode>> searchText(String title) {
        return o._searchText(title);
    }

    protected List<Tuple2<Float, BaseNode>> _searchText(String title) {
        List<Tuple2<Float, BaseNode<?>>> pre = _search("{ \"fuzzy_like_this\": { \"fields\":[\"name\",\"altName\",\"description\"], \"like_text\": \"" + title + "\" } }", BaseNode.class);
        List<Tuple2<Float, BaseNode>> res = new ArrayList<Tuple2<Float, BaseNode>>();
        for (Tuple2<Float, BaseNode<?>> tup : pre) {
            if (tup._2() instanceof BaseNode) {
                res.add(new Tuple2<Float, BaseNode>(tup._1(), (BaseNode) tup._2()));
            }
        }
        return res;
    }

    protected  List<Tuple2<Float, BaseNode<?>>> _search(String s, Class... indicies) {

		if (smr instanceof IndexingRepository) {
			String[] i =  package$.MODULE$.getIndiciesJ(indicies);
			SearchResponse r = ((IndexingRepository) smr).getElasticClient().prepareSearch().setIndices(i).setQuery(s).execute().actionGet();
			SearchHit[] ss = r.getHits().getHits();
			Map<Float, BaseNode<?>> b = new HashMap<Float, BaseNode<?>>();
			for (SearchHit sh : ss) {
				b.put(sh.getScore(), getById(sh.getId()));
			}
			return package$.MODULE$.resultsListJ(b);
		} else throw new WrongRepositoryTypeException();
    }

	class WrongRepositoryTypeException extends RuntimeException {}

    private void initializeRepository(boolean indexInitialSourcesOnBoot, boolean indexNewData, Class[] classes, Package[] packages, RDFSource... sources) {
        setSources(sources);
        sessionFactory = new SessionFactoryImpl();


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
                if ((!indexInitialSourcesOnBoot) && smr instanceof IndexingRepository) {
                    logger.info("Disabling elasticsearch index");
                    ss.add(new StatementImpl(getValueFactory().createURI("cmd:##noindex"), IndexingSail.CMD_NOINDEX, getValueFactory().createLiteral(true)));
                }
                ss.add(s.openStream(), NS, formatConvert(s.getFormat()));
                if ((!indexInitialSourcesOnBoot) && (indexNewData) && smr instanceof IndexingRepository) {
                    logger.info("Re-enabling elasticsearch index");
                    ss.add(new StatementImpl(getValueFactory().createURI("cmd:##reindex"), IndexingSail.CMD_REINDEX, getValueFactory().createLiteral(true)));
                    ss.commit();
                }
            }

            System.gc();

        } catch (Exception e) {
            e.printStackTrace();
            logger.info(e.getMessage());
            for (StackTraceElement ste : e.getStackTrace()) logger.info(ste.toString());
        }


    }

    private List<Tuple2<Float, BaseNode<?>>> searchText(String text, Class... cls) {
		if (smr instanceof IndexingRepository) {
			List<Tuple2<Float, BaseNode<?>>> pre = search("{ \"fuzzy_like_this\": { \"fields\":[\"name\",\"altName\",\"description\"], \"like_text\": \"" + text + "\" } }", cls);
			List<Tuple2<Float, BaseNode<?>>> res = new ArrayList<Tuple2<Float, BaseNode<?>>>();
			for (Tuple2<Float, BaseNode<?>> tup : pre) {
				if (tup._2() instanceof BaseNode<?>) {
					res.add(new Tuple2<Float, BaseNode<?>>(tup._1(), (BaseNode<?>) tup._2()));
				}
			}
			return res;
		} else throw new WrongRepositoryTypeException();
    }

    private RDFFormat formatConvert(Format f) {
        if (f.equals(Format.N3)) return RDFFormat.N3;
        else if (f.equals(Format.NTRIPLES)) return RDFFormat.NTRIPLES;
        else if (f.equals(Format.RDFXML)) return RDFFormat.RDFXML;
        else if (f.equals(Format.TRIG)) return RDFFormat.TRIG;
        else if (f.equals(Format.TURTLE)) return RDFFormat.TURTLE;
        else return RDFFormat.RDFXML;
    }

    private void setupOntology() {
        for (MappedClass mc : configuration.getMappedClasses()) {
            for (MappedClass msc : mc.getMappedSuperClasses()) {
                addTripleBase(new UID(NS, mc.getJavaClass().getSimpleName()), RDFS.subClassOf, new UID(NS, msc.getJavaClass().getSimpleName()));
            }
            addTripleBase(new UID(NS, mc.getJavaClass().getSimpleName()), RDFS.subClassOf, new UID(NS, mc.getJavaClass().getSimpleName()));
        }
    }

    private void setSources(RDFSource... r) {
        if (smr instanceof SesameRepository) {
            ((SesameRepository) smr).setSources(r);
        } 
    }
	
	public static void addTriple(UID s, UID p, UID oo) {
		o._addTriple(s,p,oo);
	}

    protected void _addTriple(UID s, UID p, UID o) {
        _addTriple(s, p, o, _getRepCon());
    }

    private void addTripleBase(UID s, UID p, UID o) {
        try {
            if (smr instanceof IndexingRepository)
                _addTriple(s, p, o, _getRepCon());
            else _addTriple(s, p, o, _getRepCon());
        } catch (Exception e) {
            logger.error(e.getMessage());
            for (StackTraceElement see : e.getStackTrace()) logger.error(see.toString());
        }
    }

    private void _addTriple(UID s, UID p, UID o,  RepositoryConnection sc) {
        if (smr instanceof SesameRepository) {
            ValueFactory vf = _getValueFactory();
            try {
                sc.add(
                  vf.createStatement(vf.createURI(s.getNamespace(), s.getLocalName()), vf.createURI(p.getNamespace(), p.getLocalName()), vf.createURI(o.getNamespace(), o.getLocalName()))
                );
            } catch (org.openrdf.repository.RepositoryException re) {
                throw new RepositoryException(re);
            }
        } 
    }

    public static void addTriple(UID s, UID p, Object l) {
        o._addTriple(s,p,l);
    }

    protected void _addTriple(UID s, UID p, Object l) {
        if (smr instanceof SesameRepository) {
            ValueFactory vf = _getValueFactory();
            vf.createLiteral(true);
            RepositoryConnection sc = _getRepCon();
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
                sc.add(
                  vf.createStatement(vf.createURI(s.getNamespace(), s.getLocalName()), vf.createURI(p.getNamespace(), p.getLocalName()), ll)
                );
            } catch (org.openrdf.repository.RepositoryException re) {
                throw new RepositoryException(re);
            }
        } else throw new WrongRepositoryTypeException(); 
    }

    protected BaseNode _getById(String id) {
        String uri = id;
        if (!id.contains(NS)) uri = NS + id;
        return _getById(new UID(uri));
    }
	
	public static BaseNode getById(String id) {
		return o._getById(id);
	}

    protected BaseNode _getById(UID u) {
        Session s = session;
        return session.get(BaseNode.class, u);
    }
	
	public static BaseNode getById(UID u) {
		return o._getById(u);
	}

    protected <T extends BaseNode> T _getById(ID id, Class<T> c) {
        Session s = session;
        T t = null;
        try {
            t = s.get(c, id);
        } catch (Throwable h) {
            t = c.cast(_getById(id.asURI().getLocalName()));
        }
        return t;
    }
	
	public static <T extends BaseNode> T getById(ID id, Class<T> c) {
		return o._getById(id, c);
	}

    protected ValueFactory _getValueFactory() {
        try {
            return ((SesameRepository) smr).getSesameRepository().getConnection().getValueFactory();
        } catch (Throwable t) {
            return null;
        }
    }
	
	public static ValueFactory getValueFactory() {
		return o._getValueFactory();
	}

    protected RepositoryConnection _getRepCon() {
        try {
            return ((SesameRepository) smr).getSesameRepository().getConnection();
        } catch (Exception t) {
            logger.error(t.getMessage());
            for (StackTraceElement ste : t.getStackTrace()) logger.error(ste.toString());
            return null;
        }
    }
	
	public static RepositoryConnection getRepCon() {
        logger.info(o.toString());
		return o._getRepCon();
	}

     protected <T extends BaseNode> List<T> sparqlFind(String qry, String idVar, Class<T> c, boolean first) {
        CloseableIterator<Map<String,NODE>> res = _sparql(qry);
        ArrayList<T> result = new ArrayList<T>();
        while (res.hasNext()) {
            for (Map.Entry<String, NODE> n : res.next().entrySet()) {
                if (n.getKey().equals(idVar.replace("?",""))) {
                    if (n.getValue().isResource()) {
						result.add(_getById(n.getValue().asResource(), c));
						if (first) 
							return result;
						 else 
							break;
                    }
                }
            }
        }
        return result;
    }
	
	protected <T extends BaseNode> T _sparqlFindFirst(String qry, String idVar, Class<T> c) {
		List<T> a = _sparqlFind(qry,idVar,c);
        if (a.size() >0) return a.get(0);
        else return null;
	}
	
	public static <T extends BaseNode> T sparqlFindFirst(String qry, String idVar, Class<T> c) {
		return o._sparqlFindFirst(qry, idVar, c);
	}


    protected <T extends BaseNode> List<T> _sparqlFind(String qry, String idVar, Class<T> c) {
		return sparqlFind(qry, idVar, c, false);
	}
	
	public static <T extends BaseNode> List<T> sparqlFind(String qry, String idVar, Class<T> c) {
		return o._sparqlFind(qry, idVar, c);
	}

    protected List<BaseNode> _sparqlFind(String qry, String idVar) {
		return sparqlFind(qry, idVar, BaseNode.class, false);
	}
	
	public static List<BaseNode> sparqlFind(String qry, String idVar) {
		return o._sparqlFind(qry, idVar);
	}

    protected CloseableIterator<Map<String,NODE>> _sparql(String q) {
      return sparql(package$.MODULE$.percentsToURIs(q, NS), QueryLanguage.TUPLE, org.openrdf.query.QueryLanguage.SPARQL);
    }
	
	public static CloseableIterator<Map<String,NODE>> sparql(String q) {
		return o._sparql(q);
	}

    private MappedClass findClass(String f) {
        Option<MappedClass> res = package$.MODULE$.findClass(f, configuration.getMappedClasses(), NS);
        if (res.isDefined()) return res.get();
        else return null;
    }

    protected void _setSparqlVerbose(boolean sparqlVerbose) {
		this.sparqlVerbose = sparqlVerbose;
	}
	
	public static void setSparqlVerbose(boolean sparqlVerbose) {
		o._setSparqlVerbose(sparqlVerbose);
	}

    private CloseableIterator<Map<String,NODE>> sparql(String q, QueryLanguage queryType, org.openrdf.query.QueryLanguage language) {
        String f = "prefix :    <" + NS + "> \n";
        f += "prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n";
        f += "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n";
        f += "prefix owl: <http://www.w3.org/2002/07/owl#> \n";
        f += "prefix fts: <http://rdf.useekm.com/fts#> \n";
        f += q;
        if (sparqlVerbose) logger.info(f);
        RDFConnection conn = connection;
        CloseableIterator<Map<String,NODE>> rows = null;
        try {
           rows = smr.openConnection().createQuery(QueryLanguage.SPARQL, f).getTuples();
       } catch (Exception e) {
            logger.error(e.getMessage());
            for (StackTraceElement ste : e.getStackTrace()) logger.info(ste.toString());
       }
        return rows;
    }


    private void exportXML(String fn) {
        try {
            FileOutputStream fos = new FileOutputStream(new File(fn));
            smr.export(Format.RDFXML, null, fos);
            fos.close();
        } catch (Exception e) {
           e.printStackTrace();
        }

    }

    private void loadXML(String fn) {
        try {
            FileInputStream fos = new FileInputStream(new File(fn));
            smr.load(Format.RDFXML, fos, null, false);
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private void exportTtl(String fn) {
        try {
            FileOutputStream fos = new FileOutputStream(new File(fn));
            smr.export(Format.TURTLE, null, fos);
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
	
    private void loadTtl(String fn) {

        try {
            FileInputStream fos = new FileInputStream(new File(fn));
            smr.load(Format.TURTLE, fos, null, false);
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    protected Client _getElasticClient() {
        return ((IndexingRepository) smr).getElasticClient();
    }
	
	public static Client getElasticClient() {
		return o._getElasticClient();
	}

    protected ElasticSearchIndexerSettings _elasticSettings() {

        return ((IndexingRepository) smr).getIndexer().getSettings();
    }
	
	public static ElasticSearchIndexerSettings elasticSettings() {
		return o._elasticSettings();
	}

    private String uri(String r) {
        return "%" + r;
    }

}
