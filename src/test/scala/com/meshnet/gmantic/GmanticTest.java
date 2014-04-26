package com.meshnet.gmantic;

import com.google.common.annotations.VisibleForTesting;
import com.mysema.commons.lang.CloseableIterator;
import com.mysema.rdfbean.annotations.ClassMapping;
import com.mysema.rdfbean.annotations.Predicate;
import com.mysema.rdfbean.model.Format;
import com.mysema.rdfbean.model.NODE;
import com.mysema.rdfbean.model.io.RDFSource;
import junit.framework.TestCase;
import org.geotoolkit.util.logging.LoggerFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static junit.framework.Assert.*;


public class GmanticTest {

    final static String NS = "http://localhost:9999/ontology/";
    final static Logger logger = Logger.getLogger("GmanticTest");
    static TestNode tn;

    @ClassMapping(ns = NS)
    static class TestNode extends BaseNode<TestNode> {
        @Predicate(ln = "testPred")
        public String testPred;
    }

    @BeforeClass
    public static void setUp() {
        InputStream is = GmanticTest.class.getResourceAsStream("base.owl");

        logger.info(is.toString() + " --");

        RDFSource s = new RDFSource(is, Format.RDFXML, "");
        File elastic = new File(GmanticTest.class.getResource("elasticsearch.yml").getFile());
        logger.info(elastic.getAbsolutePath());
        Ontology.config(NS).classes(TestNode.class).sources(s).elastic(elastic).indexInitialSourcesOnBoot(true).indexNewData(true).start();
        while (!Ontology.ready()) {}
        tn = new TestNode();
        tn.testPred = "pr";
        tn.name = "test";
        tn.save();
    }

    @Test
    public void getNodeById() {
        BaseNode b = Ontology.getById(tn.uid);
        assertTrue(b instanceof TestNode);
        assertEquals(((TestNode) b).testPred, "pr");
    }

    @Test
    public void sparql() {
        TestNode t = Ontology.sparqlFindFirst("select ?tn where { ?tn a %TestNode }", "tn", TestNode.class);
        assertEquals(t.uid, tn.uid);
    }

    @Test
    public void search() {
        List<Tuple2<Float, BaseNode>> l = Ontology.searchText("test");
        assertTrue(l.size() > 0);
        assertEquals(l.get(0)._2().name, "test");
    }

    @Test
    public void update() {
        tn.description = "howdy";
        tn.save();
        CloseableIterator<Map<String,NODE>> c = Ontology.sparql("select ?d where { ?tn a %TestNode . ?tn %description ?d }");
        assertTrue(c.hasNext());
        assertEquals(c.next().get("d").asLiteral().getValue(), "howdy");
    }

}