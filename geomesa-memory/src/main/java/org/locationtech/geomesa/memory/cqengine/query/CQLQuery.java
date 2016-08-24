package org.locationtech.geomesa.memory.cqengine.query;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.query.simple.SimpleQuery;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

public class CQLQuery<O, A> extends SimpleQuery<O, A> {
    private final Filter filter;

    public CQLQuery(Attribute<O, A> attribute, Filter filter) {
        super(attribute);
        this.filter = filter;
    }

    @Override
    protected boolean matchesSimpleAttribute(SimpleAttribute<O, A> attribute, O object, QueryOptions queryOptions) {
        return object instanceof SimpleFeature && filter.evaluate(object);
    }

    @Override
    protected boolean matchesNonSimpleAttribute(Attribute<O, A> attribute, O object, QueryOptions queryOptions) {
        return object instanceof SimpleFeature && filter.evaluate(object);
    }

    @Override
    protected int calcHashCode() {
        return 0;
    }
}
