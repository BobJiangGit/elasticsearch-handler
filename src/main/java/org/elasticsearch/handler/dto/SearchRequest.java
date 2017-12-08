package org.elasticsearch.handler.dto;

import com.google.common.collect.Lists;
import org.elasticsearch.handler.constants.SearchConstants;
import org.elasticsearch.handler.enums.QueryType;

import java.io.Serializable;
import java.util.List;

/**
 *
 * Created by Bob Jiang on 2016/10/27.
 */
public class SearchRequest implements Serializable {

    private static final long serialVersionUID = -1775272965412280972L;

    // DSL
    private String query;
    //索引 可为空
    private List<String> indexName = Lists.newArrayList();
    //类型 可为空
    private List<String> indexType = Lists.newArrayList();
    //匹配值
    private Object value;
    //字段 should匹配
    private List<SearchField> should;
    //字段 must匹配
    private List<SearchField> must;
    //字段 mustNot匹配
    private List<SearchField> mustNot;
    //排序 可为空
    private List<SearchOrder> order;
    //分页 可为空
    private SearchPage page;
    //高亮 可为空
    private Highlight highlight;

    /**
     * 高亮
     */
    public class Highlight {

        //是否高亮 默认不高亮
        private boolean enable = SearchConstants.SEARCH_HIGHLIGHT_ENABLE;
        //高亮标签前缀
        private String preTag = SearchConstants.HIGHLIGHT_PRE_TAG;
        //高亮标签后缀
        private String postTag = SearchConstants.HIGHLIGHT_POST_TAG;
        //高亮属性名称
        private String highlightFieldName;

        public Highlight() {}

        public Highlight(String highlightFieldName) {
            this.highlightFieldName = highlightFieldName;
            this.enable = true;
        }

        public Highlight(String highlightFieldName, String preTag, String postTag) {
            this(highlightFieldName);
            this.preTag = preTag;
            this.postTag = postTag;
        }

        public boolean getEnable() {
            return enable;
        }

        public void setEnable(boolean enable) {
            this.enable = enable;
        }

        public String getPreTag() {
            return preTag;
        }

        public void setPreTag(String preTag) {
            this.preTag = preTag;
        }

        public String getPostTag() {
            return postTag;
        }

        public void setPostTag(String postTag) {
            this.postTag = postTag;
        }

        public String getHighlightFieldName() {
            return highlightFieldName;
        }

        public void setHighlightFieldName(String highlightFieldName) {
            this.highlightFieldName = highlightFieldName;
        }

        @Override
        public String toString() {
            return "Highlight{" +
                    "enable=" + enable +
                    ", preTag='" + preTag + '\'' +
                    ", postTag='" + postTag + '\'' +
                    ", highlightFieldName='" + highlightFieldName + '\'' +
                    '}';
        }
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Highlight getHighlight() {
        return highlight;
    }

    public void setHighlight(Highlight highlight) {
        this.highlight = highlight;
    }

    public List<SearchOrder> getOrder() {
        return order;
    }

    public void setOrder(List<SearchOrder> order) {
        this.order = order;
    }

    public List<String> getIndexName() {
        return indexName;
    }

    public void setIndexName(List<String> indexName) {
        this.indexName = indexName;
    }

    public List<String> getIndexType() {
        return indexType;
    }

    public void setIndexType(List<String> indexType) {
        this.indexType = indexType;
    }

    public void setOneIndexName(String indexName) {
        this.indexName.add(indexName);
    }

    public void setOneIndexType(String indexType) {
        this.indexType.add(indexType);
    }

    public void setOneOrder(SearchOrder order) {
        if (this.order != null) {
            this.order.add(order);
        }
    }

    public SearchPage getPage() {
        return page;
    }

    public void setPage(SearchPage page) {
        this.page = page;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public List<SearchField> getShould() {
        return should;
    }

    public void setShould(List<SearchField> should) {
        this.should = should;
    }

    public List<SearchField> getMust() {
        return must;
    }

    public void setMust(List<SearchField> must) {
        this.must = must;
    }

    public List<SearchField> getMustNot() {
        return mustNot;
    }

    public void setMustNot(List<SearchField> mustNot) {
        this.mustNot = mustNot;
    }

    public int getSearchFieldSize() {
        return allFields().size();
    }

    public long getMatchAllFieldSize() {
        return allFields().stream().filter(f -> QueryType.match_all.equals(f.getQueryType())).count();
    }

    private List<SearchField> allFields() {
        List<SearchField> allFields = Lists.newArrayList();
        if (must != null && must.size() > 0) {
            allFields.addAll(must);
        }
        if (should != null && should.size() > 0) {
            allFields.addAll(should);
        }
        if (mustNot != null && mustNot.size() > 0) {
            allFields.addAll(mustNot);
        }
        return allFields;
    }

    @Override
    public String toString() {
        return "{ indexName=" + indexName +
                ", indexType=" + indexType +
                ", query='" + query + '\'' +
                ", value=" + value +
                ", should=" + should +
                ", must=" + must +
                ", mustNot=" + mustNot +
                ", order=" + order +
                ", page=" + page +
                ", highlight=" + highlight +
                '}';
    }
}
