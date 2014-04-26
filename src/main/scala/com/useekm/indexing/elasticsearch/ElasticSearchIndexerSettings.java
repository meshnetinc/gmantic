/*
 * Copyright 2012 by TalkingTrends (Amsterdam, The Netherlands)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://opensahara.com/licenses/apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.useekm.indexing.elasticsearch;

import info.aduna.iteration.CloseableIteration;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.UpdateExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.useekm.indexing.AbstractIndexerSettings;
import com.useekm.indexing.GeoConstants;
import com.useekm.indexing.IndexingSail;
import com.useekm.indexing.IndexingSailConnection;
import com.useekm.indexing.algebra.IndexerExpr;
import com.useekm.indexing.elasticsearch.IndexConfiguration.Mapping;
import com.useekm.indexing.elasticsearch.IndexConfiguration.PropertyConfig;
import com.useekm.indexing.exception.IndexException;
import com.useekm.indexing.internal.Indexer;
import com.useekm.indexing.internal.resolvers.AbstractResolveSearchArg;
import com.useekm.indexing.internal.resolvers.SearchByLiteral;
import com.useekm.indexing.internal.resolvers.SearchNoArg;
import com.useekm.indexing.internal.resolvers.SearchNotIndexed;
import com.useekm.types.GeoConvert;
import com.useekm.types.exception.InvalidGeometryException;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Settings for {@link ElasticSearchIndexer}.
 * 
 * @see IndexingSail
 */
public class ElasticSearchIndexerSettings extends AbstractIndexerSettings {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchIndexerSettings.class);
    private static final String OBJECT = "object";
    private static final String SHORT = "short";
    private static final String STRING = "string";
    private static final String LONG = "long";
    private static final String INTEGER = "integer";
    private static final String FLOAT = "float";
    private static final String DOUBLE = "double";
    private static final String DATE = "date";
    private static final String BYTE = "byte";
    private static final String BOOLEAN = "boolean";
    private static final String GEO_SHAPE = "geo_shape";
    public static final String NOT_ANALYZED = "not_analyzed";
    public static final String NOT_INDEXED = "no";

    public static final String FIELD_OBJECT_URI = "u";
    public static final String FIELD_OBJECT_TYPE = "a";

    public static final String FILTER_EDGE_NGRAM = "useekmEdgeNgram";
    public static final String ANALYZER_AUTOCOMPLETE_INDEX = "useekmAutocompleteIndex";
    public static final String ANALYZER_AUTOCOMPLETE_SEARCH = "useekmAutocompleteSearch";
    public static final String SUFFIX_ENGRAM = "engram";

    private Client client;
    private Map<String, IndexConfiguration> indexConfigurations;
    private Map<String, String> analyzers;

    private volatile Map<String, Collection<String>> toSub; //used (safe dbl checked locking fashion) outside locking in first init, hence the volatile
    private Map<String, String> toSuper;
    //TODO: the indexer should check that every type maps to only one index-configuration always

    /**
     * SPARQL function for an ElasticSearch query_string query. See {@link QueryBuilders#queryString(String)}.
     */
    public static final URI ES_QUERY = new URIImpl(NS_FTS + "query");
    /**
     * SPARQL function for an ElasticSearch match_all filter. See {@link FilterBuilders#matchAllFilter()}.
     * This is useful when combined with basic graph patterns (BGP) on indexed statements for the searched resource, because these BGP's can be moved to the ElasticSerach filter
     * for a much better performing search.
     */
    public static final URI ES_MATCH_ALL = new URIImpl(NS_FTS + "matchAll");
    /**
     * SPARQL function for an ElasticSearch query to find auto-complete suggestions for a given string.
     * This requires the use of {@link Mapping#Autocomplete} secondary mappings for properties of the searched subjects.
     */
    public static final URI ES_AUTO_COMPLETE = new URIImpl(NS_FTS + "autoComplete");
    /**
     * Option to set the size (number of results to return from an ElasticSearch). If you want to get all results, you need to use the {@link #ES_OPTION_SCROLL} option.
     * The option is set like this:
     * 
     * <pre>
     *    ?resource es:all ?match.
     *    ?match es:size 100.
     *    FILTER(es:matchAll(?match))
     * </pre>
     */
    public static final URI ES_OPTION_SIZE = new URIImpl(NS_FTS + "size");
    /**
     * Option to make a scroll search (returns all possible results for the search, instead of the top xx (where xx is es:size).
     * This creates a search of type {@link SearchType#SCAN}. The value should be a valid {@link TimeValue} according to {@link TimeValue#parseTimeValue(String, TimeValue)}.
     * The option is set like this:
     * 
     * <pre>
     *    ?resource es:all ?match.
     *    ?match es:scroll "10m".
     *    FILTER(es:matchAll(?match))
     * </pre>
     */
    public static final URI ES_OPTION_SCROLL = new URIImpl(NS_FTS + "scroll");
    /**
     * The default size for searches, when not customized via the {@link #ES_OPTION_SIZE} option.
     */
    public static final int DEFAULT_SIZE = 100; //TODO

    private static final Map<URI, AbstractResolveSearchArg> SUPPORTED_FUNCTIONS = new HashMap<URI, AbstractResolveSearchArg>();
    static {
        SUPPORTED_FUNCTIONS.put(ES_QUERY, SearchByLiteral.INSTANCE);
        SUPPORTED_FUNCTIONS.put(TEXT, SearchByLiteral.INSTANCE);
        SUPPORTED_FUNCTIONS.put(ES_MATCH_ALL, SearchNoArg.INSTANCE);
        SUPPORTED_FUNCTIONS.put(ES_AUTO_COMPLETE, SearchByLiteral.INSTANCE);
        //Geometry functions are supported (both indexed and non-indexed),
        // they can be inlined into the indexed filter/query by ExpressionExtractorOptimizer.
        // Therefore QueryExtractor does not need to handle them:
        SUPPORTED_FUNCTIONS.put(GEOF_SF_WITHIN, SearchNotIndexed.INSTANCE);
        SUPPORTED_FUNCTIONS.put(GEOF_SF_INTERSECTS, SearchNotIndexed.INSTANCE);
        SUPPORTED_FUNCTIONS.put(GEOF_SF_OVERLAPS, SearchNotIndexed.INSTANCE);
        SUPPORTED_FUNCTIONS.put(GEOF_SF_CROSSES, SearchNotIndexed.INSTANCE);
        SUPPORTED_FUNCTIONS.put(EXT_COVERED_BY, SearchNotIndexed.INSTANCE);
        SUPPORTED_FUNCTIONS.put(EXT_COVERS, SearchNotIndexed.INSTANCE);
        SUPPORTED_FUNCTIONS.put(GEOF_SF_CONTAINS, SearchNotIndexed.INSTANCE);
        SUPPORTED_FUNCTIONS.put(EXT_CONTAINS_PROPERLY, SearchNotIndexed.INSTANCE);
        SUPPORTED_FUNCTIONS.put(GEOF_SF_EQUALS, SearchNotIndexed.INSTANCE);
        SUPPORTED_FUNCTIONS.put(GEOF_SF_DISJOINT, SearchNotIndexed.INSTANCE);
        SUPPORTED_FUNCTIONS.put(GEOF_SF_TOUCHES, SearchNotIndexed.INSTANCE);
        SUPPORTED_FUNCTIONS.put(GEOF_RELATE, SearchNotIndexed.INSTANCE);
    }
    static final Map<Mapping, EsMapping[]> TYPE_MAPPINGS = new HashMap<IndexConfiguration.Mapping, EsMapping[]>();
    static {
        //primary mappings:
        TYPE_MAPPINGS.put(Mapping.Object, new EsMapping[] {new EsMapping((URI)null, null, OBJECT, NOT_ANALYZED).uri()});
        TYPE_MAPPINGS.put(Mapping.Boolean, new EsMapping[] {new EsMapping(XMLSchema.BOOLEAN, null, BOOLEAN)});
        TYPE_MAPPINGS.put(Mapping.Byte, new EsMapping[] {new EsMapping(XMLSchema.BYTE, null, BYTE)});
        TYPE_MAPPINGS.put(Mapping.Date, new EsMapping[] {new EsMapping(XMLSchema.DATE, null, DATE)});
        TYPE_MAPPINGS.put(Mapping.DateTime, new EsMapping[] {new EsMapping(XMLSchema.DATETIME, null, DATE)});
        TYPE_MAPPINGS.put(Mapping.Double, new EsMapping[] {new EsMapping(XMLSchema.DOUBLE, null, DOUBLE)});
        TYPE_MAPPINGS.put(Mapping.Float, new EsMapping[] {new EsMapping(XMLSchema.FLOAT, null, FLOAT)});
        TYPE_MAPPINGS.put(Mapping.Integer, new EsMapping[] {new EsMapping(XMLSchema.INT, null, INTEGER)});
        TYPE_MAPPINGS.put(Mapping.Long, new EsMapping[] {new EsMapping(XMLSchema.LONG, null, LONG)});
        TYPE_MAPPINGS.put(Mapping.Label, new EsMapping[] {new EsMapping((URI)null, null, STRING, NOT_ANALYZED)});
        TYPE_MAPPINGS.put(Mapping.Short, new EsMapping[] {new EsMapping(XMLSchema.SHORT, null, SHORT)});
        TYPE_MAPPINGS.put(Mapping.Text, new EsMapping[] {new EsMapping(XMLSchema.STRING, null, STRING)});
        TYPE_MAPPINGS.put(Mapping.URI, new EsMapping[] {new EsMapping((URI)null, null, STRING, NOT_ANALYZED).uri()});
        //TODO: don't store Envelope values in _source field?
        TYPE_MAPPINGS.put(Mapping.Geometry, new EsMapping[] {new EsMapping(GeoConstants.GEO_SUPPORTED, null, GEO_SHAPE)});
        //secondary mappings:
        TYPE_MAPPINGS.put(Mapping.Autocomplete, new EsMapping[] {new EsMapping((URI)null, SUFFIX_ENGRAM, STRING, null, ANALYZER_AUTOCOMPLETE_INDEX, ANALYZER_AUTOCOMPLETE_SEARCH,
            false)});
    }

    /**
     * Creates an instance for a specified elasticsearch {@link Client}.
     */
    public ElasticSearchIndexerSettings(Client client) {
        this.client = client;
        this.indexConfigurations = new HashMap<String, IndexConfiguration>();
    }

    public Client getClient() {
        return client;
    }

    void initInference(SailConnection conn, ValueFactory vf) throws SailException {
        synchronized (this) {
            if (toSub == null) {
                boolean done = false;
                try {
                    toSub = new HashMap<String, Collection<String>>();
                    toSuper = new HashMap<String, String>();
                    for (IndexConfiguration config: indexConfigurations.values()) {
                        String type = config.getMatchType();
                        initInference(conn, vf, type);
                    }
                    done = true;
                } finally {
                    if (!done) {
                        //Don't leave half configured information one failure:
                        toSub = null;
                        toSuper = null;
                    }
                }
            }
        }
    }

    void initInference(SailConnection conn, ValueFactory vf, String type) throws SailException {
        synchronized (this) {
            Collection<String> subs = toSub.get(type);
            if (subs != null)
                for (String sub: subs)
                    toSuper.remove(sub);
            subs = new HashSet<String>();
            toSub.put(type, subs);
            boolean subClassOfSelf = false;
            CloseableIteration<? extends Statement, SailException> sts =
                conn.getStatements(null, RDFS.SUBCLASSOF, vf.createURI(type), IndexConfiguration.MATCH_TYPE_INFERENCE);
            try {
                while (sts.hasNext()) {
                    String subType = sts.next().getSubject().stringValue();
                    subs.add(subType);
                    toSuper.put(subType, type);
                    if (subType.equals(type))
                        subClassOfSelf = true;
                }
            } finally {
                sts.close();
            }
            if (!subClassOfSelf) {
                Validate.isTrue(subs.isEmpty()); //no type information for this type in repository yet
                subs.add(type);
                toSuper.put(type, type);
            }
        }
    }

    /**
     * @return The indexconfiguration with the specified name (the name maps to an elasticsearch document type)
     */
    public IndexConfiguration getIndexConfiguration(String name) {
        return indexConfigurations.get(name);
    }

    /**
     * @return A collection of all {@link IndexConfiguration}s that match the provided (not null!) pred and obj.
     */
    public IndexConfiguration getIndexConfiguration(URI type) {
        Validate.notNull(type);
        String strType = type.stringValue();
        for (IndexConfiguration config: indexConfigurations.values()) {
            if (config.getMatchType().equals(strType))
                return config;
        }
        synchronized (this) {
            if (toSuper != null) {// else not initialized yet, can't return based on type inference
                String indexedType = toSuper.get(strType);
                if (indexedType != null)
                    for (IndexConfiguration config: indexConfigurations.values())
                        if (config.getMatchType().equals(indexedType))
                            return config;
            }
        }
        return null;
    }

    /**
     * @return True if at least one {@link IndexConfiguration} matches the provided (not null!) pred and obj.
     */
    boolean hasIndexConfiguration(URI type) {
        return getIndexConfiguration(type) != null;
    }

    @SuppressWarnings("unchecked")//EMPTY_LIST
    List<IndexConfiguration> getConfigsForProperty(URI pred) {
        List<IndexConfiguration> result = null;
        for (IndexConfiguration config: indexConfigurations.values())
            if (config.isProperty(pred)) {
                if (result == null)
                    result = new ArrayList<IndexConfiguration>();
                result.add(config);
            }
        return result == null ? Collections.EMPTY_LIST : result;
    }

    public void setIndexConfigurations(Collection<IndexConfiguration> configs) {
        indexConfigurations.clear();
        for (IndexConfiguration config: configs)
            if (indexConfigurations.containsKey(config.getName()))
                throw new IllegalArgumentException("Multiple index configurations with name: " + config.getName());
            else
                indexConfigurations.put(config.getName(), config);
    }

    public Collection<IndexConfiguration> getIndexConfigurations() {
        return indexConfigurations.values();
    }
    
    public void setAnalyzers(Map<String, String> analyzers) {
        this.analyzers = analyzers;
    }

    @SuppressWarnings("unchecked")//EMPTY_MAP
    public Map<String, String> getAnalyzers() {
        return analyzers == null ? (Map<String, String>)Collections.EMPTY_MAP : Collections.unmodifiableMap(analyzers);
    }

    @Override public boolean isSafeForWrappedSailEvaluation(UpdateExpr expr) {
        //TODO room for optimization by checking whether the update does not query for nor possibly affect indexed statements
        //  (see e.g. how SimpleTypeInferencingSailConnection checks this):
        return false;
    }

    @Override public Indexer createIndexer(IndexingSailConnection connection) throws SailException {
        if (toSub == null)
            initInference(connection.getWrappedConnection(), connection.getValueFactory());
        return new ElasticSearchIndexer(this, connection);
    }

    @Override public void initialize(boolean emptyIndexes) {
        try {
            Map<String, Map<String, XContentBuilder>> indexMappings = getIndexMappings();
            Collection<String> existingIndexes = new HashSet<String>();
            for (String index: indexMappings.keySet())
                if (client.admin().indices().prepareExists(index).execute().actionGet().isExists())
                    if (emptyIndexes)
                        client.admin().indices().prepareDelete(index).execute().actionGet();
                    else
                        existingIndexes.add(index);
            for (String index: indexMappings.keySet())
                if (!existingIndexes.contains(index))
                    createIndex(index, indexMappings.get(index));
            checkClusterState();
            LOG.info("ES-STATUS: index should be ready now");
        } catch (IOException e) {
            throw new IndexException(e);
        } catch (ElasticSearchException e) {
            throw new IndexException(e);
        }
    }

    public void checkClusterState() {
        //test case config does not reach a yellow/green status...
        if (client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet(60000).isTimedOut())
            throw new IndexException("ElasticSearchCluster not available");
        //client.admin().cluster().health(new ClusterHealthRequest().waitForActiveShards(1)).actionGet();
    }

    private void createIndex(String index, Map<String, XContentBuilder> mappings) throws IOException {
        CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate(index);
        builder.setSettings(createSettings("-1"));
        for (String configName: mappings.keySet())
            builder.addMapping(configName, mappings.get(configName));
        builder.execute().actionGet();
        getClient().admin().indices().prepareRefresh(index).execute().actionGet();
    }

    private Map<String, Map<String, XContentBuilder>> getIndexMappings() throws IOException {
        Map<String, Map<String, XContentBuilder>> indexMappings = new HashMap<String, Map<String, XContentBuilder>>();
        for (IndexConfiguration config: indexConfigurations.values()) {
            Map<String, XContentBuilder> mappings = indexMappings.get(config.getIndexName());
            if (mappings == null) {
                mappings = new HashMap<String, XContentBuilder>();
                indexMappings.put(config.getIndexName(), mappings);
            }
            mappings.put(config.getName(), createMapping(config));
        }
        return indexMappings;
    }

    public XContentBuilder createSettings(String refreshInterval) throws IOException {
        XContentBuilder settings = XContentFactory.jsonBuilder();
        settings.startObject();
        if (refreshInterval != null)
            settings.field("refresh_interval", refreshInterval);
        settings.startObject("analysis");
        settings.startObject("filter");
        settings.startObject(FILTER_EDGE_NGRAM);
        settings.field("max_gram", 16);
        settings.field("min_gram", 2);
        settings.field("type", "edge_ngram");
        settings.endObject(); //FILTER_EDGE_NGRAM
        settings.endObject(); //filter
        settings.startObject("analyzer");
        settings.startObject(ANALYZER_AUTOCOMPLETE_INDEX);
        settings.array("filter", "lowercase", "asciifolding", FILTER_EDGE_NGRAM);
        settings.field("tokenizer", "letter");
        settings.endObject(); //ANALYZER_AUTOCOMPLETE_INDEX
        settings.startObject(ANALYZER_AUTOCOMPLETE_SEARCH);
        settings.array("filter", "lowercase", "asciifolding"); //should be same as above, without FILTER_EDGE_NGRAM
        settings.field("tokenizer", "letter");
        settings.endObject(); //ANALYZER_AUTOCOMPLETE_SEARCH
        for (Map.Entry<String, String> entry: getAnalyzers().entrySet())
            settings.rawField(entry.getKey(), entry.getValue().getBytes());
        settings.endObject(); //analyzer
        settings.endObject(); //analysis
        settings.endObject(); //settings
        return settings;
    }

    private XContentBuilder createMapping(IndexConfiguration config) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        mapping.startObject(config.getName());
        mapping.field("date_detection", "false");
        mapping.startObject("properties");
        mapping.startObject(FIELD_OBJECT_TYPE);
        mapping.field("type", "string");
        mapping.field("index", NOT_ANALYZED);
        mapping.endObject();//a
        Collection<String> excludeFromSource = new HashSet<String>();
        for (PropertyConfig property: config.getProperties())
            createMappingForProperty("", mapping, property, excludeFromSource);
        mapping.endObject();
        if (!excludeFromSource.isEmpty()) {
            mapping.startObject("_source");
            mapping.array("excludes", excludeFromSource.toArray(new String[excludeFromSource.size()]));
            mapping.endObject();
        }
        mapping.endObject();
        mapping.endObject();
        return mapping;
        //See: http://www.elasticsearch.org/guide/reference/mapping/core-types.html
    }

    private void createMappingForProperty(String path, XContentBuilder mapping, PropertyConfig property, Collection<String> sourceExclude) throws IOException {
        EsMapping[] esMappings = property.getAllEsMappings();
        if (esMappings.length > 1) {
            mapping.startObject(property.getName());
            mapping.field("type", "multi_field");
            mapping.startObject("fields");
        }
        for (EsMapping esm: esMappings)
            createMapping(path, mapping, property, sourceExclude, esm);
        if (esMappings.length > 1) {
            mapping.endObject();//fields
            mapping.endObject();//property.getName
        }
    }

    private void createMapping(String path, XContentBuilder mapping, PropertyConfig property, Collection<String> sourceExclude, EsMapping esm) throws IOException {
        String name = esm.getName(property.getName());
        mapping.startObject(name);
        mapping.field("type", esm.type);
        esm.createMappingProperties(mapping);
        if (property.getBoost() != 1.0) //TODO: per MappingInfo not per Property!
            mapping.field("boost", property.getBoost());
        if (property.isObject() && OBJECT.equals(esm.type)) {
            mapping.startObject("properties");
            mapping.startObject(FIELD_OBJECT_URI);
            mapping.field("type", STRING);
            mapping.field("index", NOT_ANALYZED);
            mapping.endObject();//property.getName()
            for (PropertyConfig sub: property.getSubProperties())
                createMappingForProperty(path + name + ".", mapping, sub, sourceExclude);
            mapping.endObject();//properties
        }
        if (property.isSourceExclude())
            sourceExclude.add(path + name);
        mapping.endObject();//esm.getName(property.getName())
    }

    @Override public Var getResultVarFromFunctionCall(URI function, List<ValueExpr> args) {
        AbstractResolveSearchArg resolver = SUPPORTED_FUNCTIONS.get(function);
        if (resolver == null)
            return null;
        return resolver.getResultVarFromFunctionCall(function, args);
    }

    @Override public boolean isIndexedConstraint(IndexerExpr expr, StatementPattern pattern) {
        return false;
    }

    public boolean isResultProperty(URI uri) {
        return ElasticSearchIndexer.PROPERTY_BINDERS.keySet().contains(uri.stringValue());
    }

    static final class EsMapping {
        private static final String FIELD_COORDINATES = "coordinates";
        private static final String TYPE_ENVELOPE = "envelope";
        private static final String FIELD_TYPE = "type";

        private final Collection<URI> typeUris;
        private final String namePostfix; // added to the fieldname if multiple es fields map to one property
        private final String type; // string, float, ...
        private final String index; // analyzed, not_analyzed, no
        private final String indexAnalyzer;
        private final String searchAnalyzer;
        private final Boolean includeInAll;// Should the field be included in the _all field. Defaults to elasticsearch default for the type (or to the parent object type setting).
        private boolean isResource; //this maps a Resource (currently only URI is supported, in the future we might support BNode for some stores)

        EsMapping(URI typeUri, String namePostfix, String type) {
            this(typeUri, namePostfix, type, null, null);
        }

        private EsMapping(URI typeUri, String namePostfix, String type, String index) {
            this(typeUri, namePostfix, type, index, null);
        }

        private EsMapping(URI[] typeUris, String namePostfix, String type) {
            this(typeUris, namePostfix, type, null, null, null, null);
        }

        private EsMapping(URI typeUri, String namePostfix, String type, String index, Boolean includeInAll) {
            this(typeUri, namePostfix, type, index, null, null, includeInAll);
        }

        private EsMapping(URI typeUri, String namePostfix, String type, String index, String indexAnalyzer, String searchAnalyzer, Boolean includeInAll) {
            this(new URI[] {typeUri}, namePostfix, type, index, indexAnalyzer, searchAnalyzer, includeInAll);
        }

        private EsMapping(URI[] typeUris, String namePostfix, String type, String index, String indexAnalyzer, String searchAnalyzer, Boolean includeInAll) {
            this.typeUris = of(typeUris);
            this.namePostfix = namePostfix;
            this.type = type;
            this.index = index;
            this.indexAnalyzer = indexAnalyzer;
            this.searchAnalyzer = searchAnalyzer;
            this.includeInAll = includeInAll;
        }

        private static Collection<URI> of(URI[] uris) {
            if (uris == null)
                return null;
            if (uris.length == 1) {
                if (uris[0] == null)
                    return null;
                return Collections.singleton(uris[0]);
            }
            HashSet<URI> result = new HashSet<URI>(uris.length);
            for (URI uri: uris)
                result.add(uri);
            return result;
        }

        public void createMappingProperties(XContentBuilder mapping) throws IOException {
            if (index != null)
                mapping.field("index", index);
            if (includeInAll != null)
                mapping.field("include_in_all", includeInAll);
            if (indexAnalyzer != null) {
                if (searchAnalyzer != null) {
                    if (indexAnalyzer.equals(searchAnalyzer))
                        mapping.field("analyzer", indexAnalyzer);
                    else {
                        mapping.field("index_analyzer", indexAnalyzer);
                        mapping.field("search_analyzer", searchAnalyzer);
                    }
                } else
                    mapping.field("index_analyzer", indexAnalyzer);
            } else if (searchAnalyzer != null)
                mapping.field("search_analyzer", searchAnalyzer);
        }

        private EsMapping uri() {
            this.isResource = true;
            return this;
        }

        public String getName(String name) {
            if (namePostfix != null)
                return namePostfix;
            return name;
        }

        public Object getStoreValue(Resource resource, URI predicate, Value object) {
            if (object instanceof BNode)
                throw error("Indexing of BNodes is not supported", resource, predicate, object);
            if (isResource) {
                if (!(object instanceof Resource))
                    throw error("Expecting URI as object", resource, predicate, object);
                return object.stringValue();
            } else {
                if (!(object instanceof Literal))
                    throw error("Expecting Literal as object", resource, predicate, object);
                Literal literal = (Literal)object;
                return createStoreValueForLiteral(resource, predicate, object, literal);
            }
        }

        private Object createStoreValueForLiteral(Resource resource, URI predicate, Value object, Literal literal) {
            //play.Logger.info("Type: " + type + " Literal type: " + literal.getDatatype().toString());

            if (literal.getDatatype() == null) {
                if (typeUris != null)
                    throw error("Expecting type to be one of " + typeUris + " for object", resource, predicate, object);
                if (literal.getLanguage() != null)
                    throw error("Language tags are not supported", resource, predicate, object);
                return object.stringValue();
            } else if (typeUris == null)
                throw error("Unexpected type " + literal.getDatatype() + " for object", resource, predicate, object);
            else
                return datatypeValue(literal);
        }

        public Object getFilterValue(Value object) {
            //TODO: this should be relaxed to also use non-matching types in filters (should be reflected in the filter outcome)
            //TODO: types of both the filter value and possible index values should be checked, and the filter changed accordingly
            //TODO: how to handle language tags in filters?
            //TODO: numeric string values should be treated as strings, not numbers...
            if (object instanceof BNode)
                throw error("Filtering on BNodes is not supported", object);
            if (object instanceof URI)
                return object.stringValue();
            else {
                Literal literal = (Literal)object;
                if (typeUris == null)
                    return object.stringValue();
                return datatypeValue(typeUris.iterator().next(), literal);
            }
        }

        private Object datatypeValue(Literal literal) {
            for (URI uri: typeUris) {
                if (uri.equals(literal.getDatatype()))
                    return datatypeValue(uri, literal);
            }
            throw error("Expecting type to be one of " + typeUris + " for object", literal);
        }

        private Object datatypeValue(URI typeUri, Literal literal) {
            if (XMLSchema.BOOLEAN.equals(typeUri))
                return literal.booleanValue();
            if (XMLSchema.BYTE.equals(typeUri))
                return literal.byteValue();
            if (XMLSchema.DATE.equals(typeUri)) //TODO ??
                return literal.calendarValue().normalize().toGregorianCalendar().getTime();
            if (XMLSchema.DATETIME.equals(typeUri))
                return literal.calendarValue().normalize().toGregorianCalendar().getTime();
            if (XMLSchema.DOUBLE.equals(typeUri))
                return literal.doubleValue();
            if (XMLSchema.FLOAT.equals(typeUri))
                return literal.floatValue();
            if (XMLSchema.INT.equals(typeUri))
                return literal.intValue();
            if (XMLSchema.LONG.equals(typeUri))
                return literal.longValue();
            if (XMLSchema.STRING.equals(typeUri))
                return literal.stringValue();
            if (GeoConvert.isSupported(typeUri))
                return asEnvelope(literal);
            Validate.isTrue(XMLSchema.SHORT.equals(typeUri));
            return literal.shortValue();
        }

        //TODO: would be better if we indexed the geometry instead of its envelope?
        //TODO: handling of envelope.isEmpty
        //TODO: handle point data with a point instead of an envelope?
        private byte[] asEnvelope(Literal literal) {
            Geometry geo;
            try {
                geo = GeoConvert.toGeometry(literal, false);
            } catch (InvalidGeometryException e) {
                throw new IllegalStateException(e);
                //TODO: we should just not index it, or index the empty geometry?
                //TODO: we should not throw IllegalStateException for user-data
            }
            Envelope envelope = geo.getEnvelopeInternal();
            StringBuilder sb = new StringBuilder();
            sb.append('{').append(FIELD_TYPE).append(":\"").append(TYPE_ENVELOPE).append("\",").append(FIELD_COORDINATES).append(":[");
            sb.append("[").append(envelope.getMinX()).append(',').append(envelope.getMaxY()).append("],");
            sb.append("[").append(envelope.getMaxX()).append(',').append(envelope.getMinY()).append("]");
            sb.append("]}");
            try {
                return sb.toString().getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException(e);
            }
        }

        private IndexException error(String message, Value field) {
            throw new IndexException(message + ": " + field.stringValue());
        }

        private IndexException error(String message, Resource resource, URI predicate, Value field) {
            if (resource != null && predicate != null)
                throw new IndexException(message + ": " + resource.stringValue() + " " + predicate.stringValue() + " " + field.stringValue());
            throw new IndexException(message + ": " + field.stringValue());
        }
    }

    /**
     * omit_norms Boolean value if norms should be omitted or not. Defaults to false.
     * omit_term_freq_and_positions Boolean value if term freq and positions should be omitted. Defaults to false.
     * analyzer The analyzer used to analyze the text contents when analyzed during indexing and when searching using a query string. Defaults to the globally configured analyzer.
     * index_analyzer The analyzer used to analyze the text contents when analyzed during indexing.
     * search_analyzer The analyzer used to analyze the field when part of a query string.
     * include_in_all Should the field be included in the _all field (if enabled). Defaults to true or to the parent object type setting.
     */
}
