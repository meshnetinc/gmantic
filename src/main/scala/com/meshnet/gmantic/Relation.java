package com.meshnet.gmantic;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.mysema.rdfbean.annotations.ClassMapping;
import com.mysema.rdfbean.annotations.Id;
import com.mysema.rdfbean.annotations.Predicate;
import com.mysema.rdfbean.model.ID;
import com.mysema.rdfbean.model.IDType;
import com.mysema.rdfbean.model.UID;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryConnection;
import play.libs.Json;

import java.util.*;

@ClassMapping(ns = "http://graph.local/ontology/")
public abstract class Relation<S extends BaseNode, T extends BaseNode> extends BaseNode<Relation<S,T>>  {

    @Predicate
    public float weight;


    final static Logger logger = LoggerFactory.getLogger(Ontology.class);

    public Relation() {
        super();
        sources = new LinkedHashSet<Source>();
    }

    @JsonIgnore
    @Predicate(ln = "hasObject")
    protected Set<T> object;

    public void setObject(Set<T> f) {
        object = f;
    }

    public void addObject(T a) {
        if (object == null) object = new LinkedHashSet<T>();
        object.add(a);
        if (objectIds == null) objectIds = new LinkedHashSet<UID>();
        objectIds.add(a.uid);
    }

    public static <R extends Relation> R create(BaseNode i, Float weight, Class<R> rel, BaseNode... ii) {
        try {
            R rt = rel.newInstance();
            rt.setSubject(i);
            rt.weight = weight;
            rt.setObject(new HashSet<BaseNode>(Arrays.asList(ii)));
            return rt;
        } catch (Exception e) {
            logger.error(e.getMessage());
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

    public void addSource(String srcName, String srcId) {
        addSource(srcName, srcId, null);
    }

    @Override
    public UID save() {
        UID a = super.save();
        if(getObject()!=null){
            for (T obj : getObject()) if ((getSubject() != null) && (obj != null))
                Ontology.addTriple(getSubject().uid, new UID(Ontology.getNamespace(), getClass().getSimpleName()), obj.uid);
        }
        return a;
    }

}
