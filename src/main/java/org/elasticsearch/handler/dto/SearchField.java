package org.elasticsearch.handler.dto;



import org.elasticsearch.handler.enums.QueryType;

import java.io.Serializable;

/**
 * 当queryType为match_all时，fieldName，value会被忽略，匹配所有。
 * 当queryType为multi_match时，可定义多个fieldName，以逗号分隔。
 * 当queryType为range时，value可定义区间值，以#号分割，左边from值，右边to值。
 *
 * Created by Bob Jiang on 2016/10/27.
 */
public class SearchField implements Serializable {

    private static final long serialVersionUID = 455871115322862858L;

    //字段名
    private String fieldName;
    //字段值 可为空
    private Object value;
    //查询方式
    private QueryType queryType = QueryType.term;

    public SearchField() {}

    public SearchField(String fieldName) {
        this.fieldName = fieldName;
    }

    public SearchField(String fieldName, QueryType queryType) {
        this.fieldName = fieldName;
        this.queryType = queryType;
    }

    public SearchField(String fieldName, Object value) {
        this.fieldName = fieldName;
        this.value = value;
    }

    public SearchField(String fieldName, Object value, QueryType queryType) {
        this(fieldName, value);
        this.queryType = queryType;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public void setQueryType(QueryType queryType) {
        this.queryType = queryType;
    }

    @Override
    public String toString() {
        return "SearchField{" +
                "fieldName='" + fieldName + '\'' +
                ", value=" + value +
                ", queryType=" + queryType +
                '}';
    }
}
