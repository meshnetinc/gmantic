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

    private long nextLocalId = 1;

    public ElasticSearchIndexer esi;
    public IndexingSail is;
    public ElasticSearchIndexerSettings settings;
    public Sail base; // holds holdsBaseRepo
    public SailRepository baseRepo; // holds baseMemStore
    public SimpleTypeInferencingSail holdsBaseRepo; // holds baseRepo
    public MemoryStore baseMemStore;
    private RDFSource metaModel;

    public IndexingRepository() {}
    public IndexingRepository(RDFSource metaModel) {
        this.metaModel = metaModel;
    }

    public IndexingSailConnection getConnection() {

        try {
            return is.getConnection();
        } catch (Exception e) {
            play.Logger.info(e.getMessage());
            for (StackTraceElement i : e.getStackTrace()) play.Logger.info(i.toString());
        }
        return null;
    }



    private NodeFactoryBean nfb;
    public Node node;
    private Client client;

    @Override
    protected org.openrdf.repository.Repository createRepository(boolean sesameInference) {

        return new SailRepository(is);
        // return new SailRepository(new MemoryStore());
    }

    @Override
    public void initialize() {

        try {
            //settings.initialize(true);

            baseMemStore = new MemoryStore();

            baseRepo = new SailRepository(baseMemStore);

            base = new RepositorySail(baseRepo);

            holdsBaseRepo = new SimpleTypeInferencingSail(base);



            setupElasticSearch();


            is = new IndexingSail(holdsBaseRepo, settings);



        } catch (Exception e) {
            play.Logger.info(e.getMessage());
            for (StackTraceElement i : e.getStackTrace()) play.Logger.info(i.toString());
        }



        try {
            is.initialize();
            super.initialize();

            //baseRepo.getConnection().add()


        } catch (Exception e) {
            play.Logger.info(e.getMessage());
        }

        try {
            esi = (ElasticSearchIndexer) is.getConnection().getIndexer();
        } catch (Exception e) {
            play.Logger.info(e.getMessage());
            for (StackTraceElement i : e.getStackTrace()) play.Logger.info(i.toString());
        }
    }

    public static Tuple2<Client, ElasticSearchIndexer> test() {
        IndexingRepository ir = new IndexingRepository();
        Client c = ir.getElasticClient();
        ir.settings = new ElasticSearchIndexerSettings(c);
        HashMap<String, String> analyzers = new HashMap<String, String>();
        analyzers.put("default", "{\"type\":\"english\"}");
        ir.settings.setAnalyzers(analyzers);
        ArrayList<IndexConfiguration> icfgs = new ArrayList<IndexConfiguration>();
        IndexConfiguration feature = new IndexConfiguration();
        feature.setName("feature");
        feature.setIndexName("feature");
        feature.setMatchType(util.Ontology.NS + "Feature");
        ArrayList<IndexConfiguration.PropertyConfig> props = new ArrayList<IndexConfiguration.PropertyConfig>();
        IndexConfiguration.PropertyConfig displayName = new IndexConfiguration.PropertyConfig();
        displayName.setName("displayName");
        displayName.setPredicate(util.Ontology.NS + "displayName");
        displayName.setMapping(IndexConfiguration.Mapping.Text);
        displayName.setMultivalue(false);
        props.add(displayName);

        feature.setProperties(props);
        icfgs.add(feature);
        ir.settings.setIndexConfigurations(icfgs);
        try {
            ir.is = new IndexingSail(new SimpleTypeInferencingSail(new RepositorySail( new SailRepository(new MemoryStore()))), ir.settings);
            ir.is.initialize();
            IndexingSailConnection isc = ir.is.getConnection();
            ValueFactory vf = isc.getValueFactory();
            int i = 0;
            for (i = 0; i < 20; i++) {
                isc.addStatement(vf.createURI(Ontology.NS + "derp" + i), vf.createURI(RDF.type.asURI().toString()), vf.createURI(Ontology.NS + "Feature"));
                isc.addStatement(vf.createURI(Ontology.NS + "derp" + i), vf.createURI(Ontology.NS + "displayName"), vf.createLiteral("test Yo" + i));
            }
            isc.commit();

            return new Tuple2<Client, ElasticSearchIndexer>(c, (ElasticSearchIndexer) isc.getIndexer());

        } catch (Exception e) {
            play.Logger.error(e.getMessage());
            for (StackTraceElement s : e.getStackTrace()) play.Logger.error(s.toString());
        }



        return null;

    }


    @Override
    public long getNextLocalId() {

        return nextLocalId++;
    }

    public Client getElasticClient() {
        if (client == null) {
            nfb = new NodeFactoryBean();
            FileSystemResource fsr = new FileSystemResource(new java.io.File("conf/elasticsearch.yml"));
            try {
                play.Logger.info(fsr.contentLength() + "  " + fsr.getFilename());
            } catch (Exception e) {
                play.Logger.info(e.getMessage());
                for (StackTraceElement i : e.getStackTrace()) play.Logger.info(i.toString());
            }
            nfb.setConfig(fsr);
            try {
                nfb.afterPropertiesSet();
            } catch (Exception e) {
                play.Logger.info(e.getMessage());
                for (StackTraceElement i : e.getStackTrace()) play.Logger.info(i.toString());
            }
            node = nfb.getObject();
            client = node.client();
        }
        return client;

    }


    public void setupElasticSearch() {
        //baseRepo = new SailRepository(base);


        getElasticClient();

        settings = new ElasticSearchIndexerSettings(client);
        HashMap<String, String> analyzers = new HashMap<String, String>();
        analyzers.put("default", "{\"type\":\"english\"}");
        settings.setAnalyzers(analyzers);
        ArrayList<IndexConfiguration> icfgs = new ArrayList<IndexConfiguration>();


        for (MappedClass mc : util.Ontology.configuration.getMappedClasses()) {

            IndexConfiguration feature = new IndexConfiguration();
           // play.Logger.info(mc.getJavaClass().getSimpleName() + " Added " + mc.getJavaClass().getName());
            feature.setName(mc.getJavaClass().getSimpleName());
            feature.setIndexName(mc.getJavaClass().getSimpleName().toLowerCase());


            try {

                String matchType = util.Ontology.NS + (mc.getJavaClass().getSimpleName());
                //matchType = matchType.substring(0, matchType.length() -1);
                feature.setMatchType(matchType);

                ArrayList<IndexConfiguration.PropertyConfig> props = new ArrayList<IndexConfiguration.PropertyConfig>();
                IndexConfiguration.PropertyConfig displayName = new IndexConfiguration.PropertyConfig();
                displayName.setName("displayName");
                displayName.setPredicate(util.Ontology.NS + "displayName");
                displayName.setMapping(IndexConfiguration.Mapping.Text);
                displayName.setMultivalue(false);
                props.add(displayName);

                IndexConfiguration.PropertyConfig description = new IndexConfiguration.PropertyConfig();
                description.setName("description");
                description.setPredicate(util.Ontology.NS + "description");
                description.setMapping(IndexConfiguration.Mapping.Text);
                description.setMultivalue(false);
                props.add(description);

                IndexConfiguration.PropertyConfig alt = new IndexConfiguration.PropertyConfig();
                alt.setName("altName");
                alt.setPredicate(util.Ontology.NS + "altName");
                alt.setMapping(IndexConfiguration.Mapping.Text);
                alt.setMultivalue(true);
                props.add(alt);

                IndexConfiguration.PropertyConfig metadata = new IndexConfiguration.PropertyConfig();
                metadata.setName("metadata");
                metadata.setPredicate(util.Ontology.NS + "metadata");
                metadata.setMapping(IndexConfiguration.Mapping.Text);
                metadata.setMultivalue(false);
                props.add(metadata);

                //(new SearchHit).

                feature.setProperties(props);

                String out = "Added " + " " + feature.getName() + "\n\t" + feature.getMatchType() + "\t" + feature.getIndexName() + "\n\t";
                for (IndexConfiguration.PropertyConfig pc : feature.getProperties()) {
                    out += pc.getName() + " - " + pc.getPredicate() + " - " + pc.getPrimaryEsMapping() + " - " + pc.getMapping().toString() + "\n\t";
                    pc.getAllEsMappings();
                }
               // play.Logger.info(out);

                icfgs.add(feature);

            } catch (Exception e) {
                play.Logger.info(e.getMessage());
                for (StackTraceElement i : e.getStackTrace()) play.Logger.info(i.toString());
            }

        }



        settings.setIndexConfigurations(icfgs);





        try {
            //  is.initialize();



        } catch (Exception e) {
            play.Logger.error(e.getMessage());
            for (StackTraceElement si : e.getStackTrace()) play.Logger.error(si.toString());
        }

    }
}