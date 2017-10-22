package org.apache.sling.pipes.internal.slingQuery;

import org.apache.sling.api.resource.Resource;
import org.apache.sling.pipes.Plumber;
import org.apache.sling.query.SlingQuery;

import static org.apache.sling.query.SlingQuery.$;

public class ClosestPipe extends AbstractExpressionSlingQueryPipe {
    public static final String RESOURCE_TYPE = RT_PREFIX + "closest";

    public ClosestPipe(Plumber plumber, Resource resource) throws Exception {
        super(plumber, resource);
    }

    @Override
    protected SlingQuery getQuery(Resource resource, String expression) {
        return $(resource).closest(expression);
    }
}
