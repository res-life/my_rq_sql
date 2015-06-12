package com.yeahmobi.datasystem.query.process;
/**
 * Created by yangxu on 3/24/14.
 */

import com.fasterxml.jackson.core.type.TypeReference;
import io.druid.query.Query;

public class QueryContext {

    Query query;
    QueryType queryType;
    TypeReference typeRef;
    TypeReference elemType;
    // add by martin 20140701 start
    String queryParam;
    // add by martin 20140701 end

    public QueryContext() {}

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public TypeReference getTypeRef() {
        return typeRef;
    }

    public void setTypeRef(TypeReference typeRef) {
        this.typeRef = typeRef;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    public TypeReference getElemType() {
        return elemType;
    }

    public void setElemType(TypeReference elemType) {
        this.elemType = elemType;
    }

    public String getQueryParam() {
        return queryParam;
    }

    public void setQueryParam(String queryParam) {
        this.queryParam = queryParam;
    }
}
