package com.meshnet.gmantic;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.mysema.commons.lang.CloseableIterator;
import com.mysema.rdfbean.annotations.ClassMapping;
import com.mysema.rdfbean.annotations.Id;
import com.mysema.rdfbean.annotations.Predicate;
import com.mysema.rdfbean.model.*;
import com.mysema.rdfbean.object.Session;
import org.joda.time.DateTime;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.*;
import play.libs.Json;

import java.util.*;

@ClassMapping(ns = "http://graph.local/ontology/")
public class BaseNode<T extends BaseNode> {

    public String className = this.getClass().getName();

    final Logger logger = LoggerFactory.getLogger(Ontology.class);

    @Id(IDType.URI)
    public UID uid;

    @Predicate(ignoreInvalid = true)
    public String name;

    @Predicate(ignoreInvalid = true)
    public String description;

	//TODO potentially add ParentOf relation and functions back in

    public BaseNode() {
        Metadata m = new Metadata();
        name = "NULL";
        description = "NULL";
        updateMetadata(m);
        String n = "node";
        try {
            n = ((Class<?>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]).getSimpleName().toLowerCase();
        } catch (Throwable e) {
            try {
                n = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0].toString();
            } catch (Throwable ex) {
            }
        }
        String ds = DateTime.now().toInstant().getMillis() + "";
        ds = ds.substring(ds.length() / 2 , ds.length() - 1);
        String rs = ((Math.random()) + "");
        rs = rs.substring(1, rs.length() / 2);
        String f = ds +rs;
        f = f.replaceAll("\\.","" );
        uid = new UID(Ontology.getNamespace() + parentsURI(), n + f);
    }

    protected String parentsURI() {
        String parents = "";
        Class sc = getClass();
        ArrayList<String> s = new ArrayList<String>();
        while (!sc.getSimpleName().equals("BaseNode"))  {
            s.add(sc.getSimpleName());
            sc = sc.getSuperclass();
        }
        Collections.reverse(s);
        for (String p : s) {
            parents += p + "/";
        }
        return parents;
    }

    public void addSource(String srcName, String srcId) {
        Metadata m = getMetadata();
        if (m.sources == null) m.sources = new ArrayList<Source>();
        m.sources.add(new Source(srcName, srcId));
        updateMetadata(m);
    }

    public void addSource(Source s) {
        Metadata m = getMetadata();
        if (m.sources == null) m.sources = new ArrayList<Source>();
        m.sources.add(s);
        updateMetadata(m);
    }

    public void addSource(String srcName, String srcId, Map<String,String> otherSrcMetadata) {
        Metadata m = getMetadata();
        if (m.sources == null) m.sources = new ArrayList<Source>();
        m.sources.add(new Source(srcName, srcId, otherSrcMetadata));
        updateMetadata(m);
    }

    public Source getSourceByName(String name) {
        List<Source> sources = getMetadata().sources;
        if (sources != null) {
            for (Source s : sources) {
                if (s.name.equals(name))  return s;
            }
        }
        return null;
    }

    public static <I extends BaseNode> List<I> findByDisplayName(String ss, Class<I> c) {
        List<I> s = Ontology.sparqlFind("select ?i where { ?i a %" + c.getSimpleName() + " . ?i %name ?d . filter(lcase(str(?d)) = \"" + ss + "\") } ", "i", c);
        return s;
    }

    public static <I extends BaseNode> I findOrCreateByDisplayName(String dn, Class<I> c) {
        List<I> s = findByDisplayName(dn, c);
        if ((s != null) && (s.size() > 0)) return s.get(0);
        else try {
            I i = c.newInstance();
            i.name = dn;
            i.save();
            return i;
        } catch (Exception e) {
            return null;
        }
    }

    public void setUser(String user) {
        Metadata m = getMetadata();
        m.user = user;
        updateMetadata(m);
    }

    public static <B extends BaseNode> List<B> findBySourceId(String id, Class<B> c) {
        return Ontology.sparqlFind("select ?x where { ?x %metadata ?m . filter(contains(?m, '\"id\":\"" + id + "\"')) }", "x", c);
    }

    public Metadata getMetadata() {
        try {
            return Json.fromJson(Json.parse(metadata), Metadata.class);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return new Metadata();
        }
    }
    private void updateMetadata(Metadata m) {
        try {
            m.update = DateTime.now();
            metadata = Json.toJson(m).toString();
            //play.Logger.info("For " + uid.toString() + " Wrote metadata " + metadata);
        } catch (Exception e) {

        }
    }

    /**
     * TODO Store actual history of this node -- List<Update> updates, DateTime creates, Update => { what changed -- allow child classes to override update fields }
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Metadata {
       public List<Source> sources;

       @JsonFormat(shape=JsonFormat.Shape.STRING, pattern="yyyy-MM-dd,HH:00", timezone="EST")
       public DateTime update;

        public Metadata() {}

        public Metadata(List<Source> sources, DateTime update, String user) {
            this.sources = sources;
            this.update = update;
            this.user = user;
        }

       public String user;
       public Metadata update() {
           update = DateTime.now();
           return this;
       }
    }

    public static BaseNode.Source createSource(String name, String id, float value, float maxValue, int n) {
        BaseNode.Source s = new BaseNode.Source(name, id);
        s.value = value;
        s.maxValue = maxValue;
        s.n = n;
        return s;
    }

    public static BaseNode.Source createSource(String name, String id, String scale, float value, float maxValue, int n) {
        BaseNode.Source s = new BaseNode.Source(name, id);
        s.value = value;
        s.maxValue = maxValue;
        s.n = n;
        s.scale = scale;
        return s;
    }

    public void addAltName(String alt) {
        if (altName == null) altName = new HashSet<String>();
        else altName.add(alt);
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Source {
        public String name;
        public String id;
        public float value;
        public float maxValue;
        public int n;
        Map<String,String> otherSrcMetadata;
        public String scale;

        public Source(String name, String id, float value, float maxValue, int n) {
            this.name = name;
            this.id = id;
            this.value = value;
            this.maxValue = maxValue;
            this.n = n;
        }

        public Source() {}

        public Source(String srcName, String srcId) {
            name = srcName;
            id = srcId;
        }
        public Source (String srcName, String srcId, Map<String,String> otherSrcMetadata) {
            name = srcName;
            id = srcId;
            this.otherSrcMetadata = otherSrcMetadata;
        }
    }

    //json
    @Predicate
    public String metadata;

    @Predicate
    public Set<String> altName;

    public String listSources() {
        String s = "";
        if (getMetadata() != null) if (getMetadata().sources != null) for (Source src : getMetadata().sources) s += src.name + ",";
        if (s.charAt(s.length()) == ',') s = s.substring(0, s.charAt(s.length() - 1));
        return s;
    }

    public UID save() {
        try {
            if (getMetadata().sources == null) {
                addSource("none", "");
            } else updateMetadata(getMetadata().update());
            return Ontology.save(this);
        } catch (Exception e) {
            e.printStackTrace();
            int i = 0;
            for (StackTraceElement s : e.getStackTrace()) {
                if (i > 3) break;
                i++;
            }
            return null;
        }
    }

}
