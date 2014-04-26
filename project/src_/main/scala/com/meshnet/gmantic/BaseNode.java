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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

@ClassMapping(ns = Ontology.NS)
public class BaseNode<T extends BaseNode> {

    public String className = this.getClass().getName();

    @Id(IDType.URI)
    public UID uid;

    @Predicate(ignoreInvalid = true)
    public String displayName;

    @Predicate(ignoreInvalid = true)
    public String description;

    public BaseNode<T> setNameSave(String name) {
        displayName = name;
        save();
        return this;
    }

    public ParentOf addParent(BaseNode parent) {
        ParentOf p = Ontology.sparqlFindFirst("select ?d where { ?d a %ParentOf . ?d %hasSubject %" + parent.uid.getLocalName() + " }", "d", ParentOf.class);
        if (p != null) {
            p.addObject(this);
            p.save();
            this.save();
        } else {
            p = new ParentOf();
            p.setSubject(parent);
            p.addObject(this);
            p.save();
            this.save();
        }
        return p;
    }

    public ParentOf addChild(BaseNode child) {
        ParentOf p = Ontology.sparqlFindFirst("select ?d where { ?d a %ParentOf . ?d %hasSubject <" + uid.toString() + "> }", "d", ParentOf.class);
        if (p != null) {
            p.addObject(child);
            p.save();
            this.save();
        } else {
            p = new ParentOf();
            p.setSubject(this);
            p.addObject(child);
            p.save();
            this.save();
        }
        return p;
    }

    public void addParent(UID parent) {
        BaseNode b = util.Ontology.getById(parent);

        if ((b != null))
            addParent(b);
    }


    public BaseNode() {

        Metadata m = new Metadata();

        displayName = "NULL";
        description = "NULL";

        updateMetadata(m);
        String n = "node";
        try {
            n = ((Class<?>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]).getSimpleName().toLowerCase();
        } catch (Throwable e) {
            try {
                n = ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0].toString();
            } catch (Throwable ex) {
                // e.printStackTrace();
            }
        }
        String ds = DateTime.now().toInstant().getMillis() + "";
        ds = ds.substring(ds.length() / 2 , ds.length() - 1);
        String rs = ((Math.random()) + "");
        rs = rs.substring(1, rs.length() / 2);
        String f = ds +rs;
        f = f.replaceAll("\\.","" );

        //play.Logger.info(util.Ontology.NS + parents);
        uid = new UID(util.Ontology.NS + parentsURI(), n + f);
    }

    protected String parentsURI() {
        String parents = "";
        Class sc = getClass();
        ArrayList<String> s = new ArrayList<String>();
        //play.Logger.info("Creating name");
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

    public T init(String id, Boolean disp) {
        //this.id = new URIImpl(util.Ontology.NS + util.common.machinize(id));
        if (disp) displayName = id;
        return (T) this;
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
        //play.Logger.info(metadata);
        if (sources != null) {
            for (Source s : sources) {
                if (s.name.equals(name))  return s;
            }
        }
        //play.Logger.info("For " + displayName + " could not find source " + name);
        return null;
    }

    public static <I extends BaseNode> List<I> findByDisplayName(String ss, Class<I> c) {
        List<I> s = Ontology.sparqlFind("select ?i where { ?i a %" + c.getSimpleName() + " . ?i %displayName ?d . filter(lcase(str(?d)) = \"" + ss + "\") } ", "i", c);
        return s;
    }

    public BaseNode<T> deduplicate(String... predicates) {
        String sparql = "select ?i where { ?i a %" + getClass().getSimpleName();
        int p = 0;
        for (String predicate : predicates) {
            sparql += " . <" + uid.toString() + "> " + predicate + " ?f" + p;
            sparql += " . ?i " + predicate + " ?i" + p;
            sparql += " . " + " filter(lcase(str(?f" + p + ")) = lcase(str(?i" + p + ")))";
            p++;
        }
        sparql += " }";
        List<? extends BaseNode> s = Ontology.sparqlFind(sparql, "i", getClass());
        if (s.size() > 0) {
            return s.get(0);
        } else return this;
    }

    public static <I extends BaseNode> I findOrCreateByDisplayName(String dn, Class<I> c) {
        List<I> s = findByDisplayName(dn, c);
        if ((s != null) && (s.size() > 0)) return s.get(0);
        else try {
            I i = c.newInstance();
            i.displayName = dn;
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
            play.Logger.error(e.getMessage());
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

    public void addAltDesc(String alt) {
        if (altDesc == null) altDesc = new HashSet<String>();
        else altDesc.add(alt);
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

    public T init(String id) {
        //this.id = new URIImpl(util.Ontology.NS + util.common.machinize(id));
        return (T) this;
    }

    //json
    @Predicate
    public String metadata;

    @Predicate
    public Set<String> altName;

    @Predicate
    public Set<String> altDesc;

   /* @Override
    public void save() {
        super.save();
        util.Ontology.session.save(this);
    }                   */


    public String listSources() {
        String s = "";
        if (getMetadata() != null) if (getMetadata().sources != null) for (Source src : getMetadata().sources) s += src.name + ",";
        if (s.charAt(s.length()) == ',') s = s.substring(0, s.charAt(s.length() - 1));
        return s;
    }

    public UID save() {
        try {
            //AppScala$.MODULE$.tellCoordinator(AppScala$.MODULE$.createMessage(toGraphView()));
            //play.Logger.info(metadata + "=metadata");
            //play.Logger.info(getMetadata().sources + "=metadata sources");
            if (getMetadata().sources == null) {
                addSource("meshnet", "");
            } else updateMetadata(getMetadata().update());
        //    play.Logger.info("Saving " + displayName + " " + uid.toString());

            return util.Ontology.save(this);
        } catch (Exception e) {
            e.printStackTrace();
            //play.Logger.error("Message: " + e.getMessage());
            int i = 0;
            for (StackTraceElement s : e.getStackTrace()) {
                if (i > 3) break;
                //play.Logger.error(s.toString());
                i++;

            }
            return null;
        }
    }

    public String toGraphView() {
        return Json.toJson(new GraphNode(displayName, this.getClass().getSimpleName())).toString();
    }

    public Long numericId() {
        return Long.parseLong(uid.getLocalName().replaceAll("[a-zA-Z]+", ""));
    }



    class GraphNode {
        public String name;
        public long id;
        public String t;
        public GraphNode(String name, String t) {
            this.name = name;
            this.id = numericId();
            this.t = t.toLowerCase();
           // play.Logger.info(name + " " + id + " " + t + " " + Json.toJson(this));
        }

    }

    /*public void saveToDB() {
        super.save();
    } */

}
