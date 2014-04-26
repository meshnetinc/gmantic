package com.meshnet.gmantic;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mysema.rdfbean.annotations.ClassMapping;
import com.mysema.rdfbean.annotations.Id;
import com.mysema.rdfbean.annotations.Predicate;
import com.mysema.rdfbean.model.ID;
import com.mysema.rdfbean.model.IDType;
import com.mysema.rdfbean.model.UID;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryConnection;

import java.util.*;

@ClassMapping(ns=util.Ontology.NS)
public abstract class Relation<S extends BaseNode, T extends BaseNode> extends BaseNode<Relation<S,T>>  {

    @Predicate
    public float weight;


    public Relation() {
        super();
        sources = new LinkedHashSet<Source>();
    }

    @JsonIgnore
    @Predicate(ln = "hasObject")
    protected Set<T> object;

    @Predicate
    public float confidence;

    public void setObject(Set<T> f) {
        object = f;
    }

    public void addObject(T a) {
        if (object == null) object = new LinkedHashSet<T>();
        object.add(a);
        if (objectIds == null) objectIds = new LinkedHashSet<UID>();
        objectIds.add(a.uid);


        //save();
    }

    public static <R extends Relation> R create(Feature i, Float weight, Class<R> rel, Feature... ii) {
        try {
            R rt = rel.newInstance();
            rt.setSubject(i);
            rt.weight = weight;
            rt.setObject(new HashSet<Feature>(Arrays.asList(ii)));
            return rt;
        } catch (Exception e) {
            play.Logger.error(e.getMessage());
            return null;
        }
    }

    public Set<T> getObject() {
        return object;
    }

    @Predicate(ln = "hasSubject")
    @JsonIgnore
    protected S subject;

    public UID subjectId;
    public Set<UID> objectIds;

    public void setSubject(S f) {
        subject = f;
        subjectId = f.uid;
    }

    public S getSubject() {
        return subject;
    }

    @Predicate(ln = "hasSource")
    public Set<Source> sources;

//    public void addSource(String srcName, String srcId, Map<String,String> otherSrcMetadata) {
//        Source s = new Source();
//        s.sourceName = srcName;
//        s.sourceId = srcId;
//        Ontology.sessionFactory.openSession().save(s);
//        if ((!(otherSrcMetadata == null)) && (!otherSrcMetadata.isEmpty())) {
//            //RepositoryConnection con = util.Ontology.getRepCon();
//            //ValueFactory vf = util.Ontology.getValueFactory();
//            for (Map.Entry<String,String> e: otherSrcMetadata.entrySet()) try {
//                //con.add(vf.createURI(id.getValue()), vf.createURI(util.Ontology.NS, e.getKey()), vf.createLiteral(e.getValue()));
//                util.Ontology.addTriple(
//                    s.uid,
//                    new UID(util.Ontology.NS, e.getKey()),
//                    e.getValue()
//                );
//            } catch (Exception ex) { }
//        }
//        sources.add(s);
//    }

    @Override
    public String toGraphView() {
        String res = "[";
        res += getSubject().toGraphView() + ",";
        for (T t : getObject()) {
            RelGraphNode rgn = new RelGraphNode(displayName, getClass().getSimpleName(), t, weight);
            res += Json.toJson(rgn).toString() + ",";
            res += Json.toJson(t.toGraphView()) + ",";
        }
        if (res.charAt(res.length() - 1) == ',') res = res.substring(0, res.length() - 1);
        res += "]";
        return res;
    }

    class RelGraphNode extends GraphNode {
        public Float weight;
        public long from;
        public long to;
        public boolean isRelation = true;
        public RelGraphNode(String name,  String t, BaseNode to, Float weight) {
            super(name,t);
            this.weight = weight * 10;
            from = getSubject().numericId();
            this.to = to.numericId();
        }
    }

    public void addSource(String srcName, String srcId) {
        addSource(srcName, srcId, null);
    }

    /*public  <T extends Relation> List<T> findBySourceId(String srcId) {
        return util.Ontology.sparqlw("?relation", (Class<T>) getClass(),
                "?relation %hasSource ?source",
                "?source %sourceId ?sourceId",
                "filter contains(?sourceId, \"" + srcId + "\")  ");
    }

    public static <T extends Relation> List<T> findBySourceId(String srcId, Class<T> cls) {
        return util.Ontology.sparqlw("?relation", cls,
                "?relation %hasSource ?source",
                "?source %sourceId ?sourceId",
                "filter contains(?sourceId, \"" + srcId + "\")");
    }*/

//    public Source getSourceByName(String name) {
//        if (sources != null) {
//            for (Source s : sources) {
//                if (s.sourceName.equals(name))  return s;
//            }
//        }
//        return null;
//    }




    public void saveRel() {
        if (object != null) for (T a : object) new Rel(subject.uid.toString(), a.uid.toString(), getClass().getSimpleName(), weight, listSources());
    }

    @Override
    public UID save() {
        UID a = super.save();
        if(getObject()!=null){
            for (T obj : getObject()) if ((getSubject() != null) && (obj != null))
                util.Ontology.addTriple(getSubject().uid, new UID(Ontology.NS, getClass().getSimpleName()), obj.uid);
        }
        return a;
    }

}
