package com.meshnet.gmantic;

import com.mysema.rdfbean.model.RDF;
import com.mysema.rdfbean.model.RDFConnection;
import com.mysema.rdfbean.model.RepositoryException;
import com.mysema.rdfbean.model.io.RDFSource;
import com.mysema.rdfbean.object.MappedClass;
import com.mysema.rdfbean.sesame.SesameConnection;
import com.mysema.rdfbean.sesame.SesameRepository;
import com.useekm.indexing.IndexingSail;
import com.useekm.indexing.IndexingSailConnection;
import com.useekm.indexing.elasticsearch.ElasticSearchIndexer;
import com.useekm.indexing.elasticsearch.ElasticSearchIndexerSettings;
import com.useekm.indexing.elasticsearch.IndexConfiguration;
import com.useekm.indexing.elasticsearch.NodeFactoryBean;
import com.useekm.inference.SimpleTypeInferencingSail;
import com.useekm.reposail.RepositorySail;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;
import org.springframework.core.io.FileSystemResource;
import scala.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

public class IndexingRepository extends SesameRepository {

	final Logger logger = LoggerFactory.getLogger(IndexingRepository.class);
    private long nextLocalId = 1;
    private ElasticSearchIndexer esi;
    private IndexingSail is;
    private ElasticSearchIndexerSettings settings;
    private Sail base; 
    private SailRepository baseRepo; 
    private SimpleTypeInferencingSail holdsBaseRepo;
    private Long initTimeout = 30000L;
    private Sail baseStore;

    private NodeFactoryBean nfb;
    private Node node;
    private Client client;
	private File elasticConf = null;
	private ElasticSearchIndexerSettings elasticSettings = null;
	private boolean defaultIndexConfigs = true;
	private MappedClass[] mappedClasses;
	private String namespace;
	
	public ElasticSearchIndexer getIndexer() {
		return esi;
	}

    public IndexingRepository(String namespace, Long ititTimeout, File elasticConf, ElasticSearchIndexerSettings elasticSettings, boolean defaultIndexConfigs, MappedClass[] mappedClasses) {
		this.elasticSettings = elasticSettings;
		this.elasticConf = elasticConf;
		this.defaultIndexConfigs = defaultIndexConfigs;
		this.baseStore = new MemoryStore();
		this.mappedClasses = mappedClasses;
		this.namespace = namespace;
        this.initTimeout = initTimeout;
	}
    public IndexingRepository(String namespace, Long ititTimeout, File elasticConf, ElasticSearchIndexerSettings elasticSettings, boolean defaultIndexConfigs, MappedClass[] mappedClasses, Sail baseStore) {
        this.baseStore = baseStore;
		this.elasticSettings = elasticSettings;
		this.elasticConf = elasticConf;
		this.defaultIndexConfigs = defaultIndexConfigs;
		this.mappedClasses = mappedClasses;
		this.namespace = namespace;
        this.initTimeout = initTimeout;
    }

    public IndexingSailConnection getConnection() {
        try {
            return is.getConnection();
        } catch (Exception e) {
            logger.error(e.getMessage());
            for (StackTraceElement i : e.getStackTrace()) logger.error(i.toString());
        }
        return null;
    }

    @Override
    protected org.openrdf.repository.Repository createRepository(boolean sesameInference) {
        return new SailRepository(is);
    }

    @Override
    public void initialize() {
        try {
            baseRepo = new SailRepository(baseStore);
            base = new RepositorySail(baseRepo);
            holdsBaseRepo = new SimpleTypeInferencingSail(base);
            setupElasticSearch();
            is = new IndexingSail(holdsBaseRepo, settings);
        } catch (Exception e) {
            logger.error(e.getMessage());
            for (StackTraceElement i : e.getStackTrace()) logger.error(i.toString());
        }
        try {
            is.initialize();
            super.initialize();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        try {
            esi = (ElasticSearchIndexer) is.getConnection().getIndexer();
            DateTime dt = new DateTime();
            while (!esi.getConn().isOpen()) {
                logger.info("derp");
                if (((new DateTime()).getMillis() - dt.getMillis()) > initTimeout) break;
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            for (StackTraceElement i : e.getStackTrace()) logger.error(i.toString());
        }
    }

    @Override
    public long getNextLocalId() {
        return nextLocalId++;
    }

    public Client getElasticClient() {
        if (client == null) {
            nfb = new NodeFactoryBean();
            FileSystemResource fsr = new FileSystemResource(elasticConf);
            nfb.setConfig(fsr);
            try {
                nfb.afterPropertiesSet();
            } catch (Exception e) {
                logger.error(e.getMessage());
                for (StackTraceElement i : e.getStackTrace()) logger.error(i.toString());
            }
            node = nfb.getObject();
            client = node.client();
        }
        return client;

    }

    public void setupElasticSearch() {
        getElasticClient();
		
		if (elasticSettings == null) {
			settings = new ElasticSearchIndexerSettings(client);
			HashMap<String, String> analyzers = new HashMap<String, String>();
			analyzers.put("default", "{\"type\":\"english\"}");
			settings.setAnalyzers(analyzers);
		} else settings = elasticSettings;
        
		if (defaultIndexConfigs) {
			ArrayList<IndexConfiguration> icfgs = new ArrayList<IndexConfiguration>();
			for (MappedClass mc : mappedClasses) {
				IndexConfiguration feature = new IndexConfiguration();
				feature.setName(mc.getJavaClass().getSimpleName());
				feature.setIndexName(mc.getJavaClass().getSimpleName().toLowerCase());
				try {
					String matchType = namespace + (mc.getJavaClass().getSimpleName());
					feature.setMatchType(matchType);

					ArrayList<IndexConfiguration.PropertyConfig> props = new ArrayList<IndexConfiguration.PropertyConfig>();
					IndexConfiguration.PropertyConfig displayName = new IndexConfiguration.PropertyConfig();
					displayName.setName("name");
					displayName.setPredicate(namespace + "name");
					displayName.setMapping(IndexConfiguration.Mapping.Text);
					displayName.setMultivalue(false);
					props.add(displayName);

					IndexConfiguration.PropertyConfig description = new IndexConfiguration.PropertyConfig();
					description.setName("description");
					description.setPredicate(namespace + "description");
					description.setMapping(IndexConfiguration.Mapping.Text);
					description.setMultivalue(false);
					props.add(description);

					IndexConfiguration.PropertyConfig alt = new IndexConfiguration.PropertyConfig();
					alt.setName("altName");
					alt.setPredicate(namespace + "altName");
					alt.setMapping(IndexConfiguration.Mapping.Text);
					alt.setMultivalue(true);
					props.add(alt);

					IndexConfiguration.PropertyConfig metadata = new IndexConfiguration.PropertyConfig();
					metadata.setName("metadata");
					metadata.setPredicate(namespace + "metadata");
					metadata.setMapping(IndexConfiguration.Mapping.Text);
					metadata.setMultivalue(false);
					props.add(metadata);

					feature.setProperties(props);

					for (IndexConfiguration.PropertyConfig pc : feature.getProperties()) {
						pc.getAllEsMappings();
					}

					icfgs.add(feature);

				} catch (Exception e) {
					logger.error(e.getMessage());
					for (StackTraceElement i : e.getStackTrace()) logger.error(i.toString());
				}

			}
			settings.setIndexConfigurations(icfgs);

        }

    }
}