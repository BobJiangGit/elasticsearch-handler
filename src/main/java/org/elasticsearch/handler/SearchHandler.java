package org.elasticsearch.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.*;
import io.searchbox.action.AbstractAction;
import io.searchbox.action.Action;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import org.elasticsearch.handler.constants.SearchConstants;
import org.elasticsearch.handler.dto.SearchField;
import org.elasticsearch.handler.dto.SearchOrder;
import org.elasticsearch.handler.dto.SearchPage;
import org.elasticsearch.handler.dto.SearchRequest;
import org.elasticsearch.handler.enums.Clause;
import org.elasticsearch.handler.enums.QueryType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Bob Jiang on 2016/10/27.
 */
public class SearchHandler {

    private static final Logger log = LoggerFactory.getLogger(SearchHandler.class);

    private volatile static SearchHandler searchHandler;

    private ClientHandler clientHandler = ClientHandler.getInstance();

    private SearchHandler() {}

    public static SearchHandler getInstance() {
        if (searchHandler == null) {
            synchronized (SearchHandler.class) {
                if (searchHandler == null) {
                    searchHandler = new SearchHandler();
                }
            }
        }
        return searchHandler;
    }

    public Search getSearch(SearchRequest searchRequest) {
        Search.Builder builder = new Search.Builder(searchRequest.getQuery());
        if (!searchRequest.getIndexName().isEmpty()) {
            builder.addIndices(searchRequest.getIndexName());
        }
        if (!searchRequest.getIndexType().isEmpty()) {
            builder.addTypes(searchRequest.getIndexType());
        }
        return builder.build();
    }

    public Search buildSearch(SearchRequest request) {
        if (request.getQuery() != null) {
            String query = request.getQuery();
            SearchPage page = request.getPage();
            if (page != null && page.getEnable()) {
                Integer size = page.getPageSize();
                Integer from = page.getPageNum() != null && page.getPageNum() != 0 ? (page.getPageNum() - 1) * size : 0;

                JSONObject json = JSON.parseObject(query);
                json.put("size", size);
                json.put("from", from);
                query = json.toJSONString();
            }

            query = buildOrder(request.getOrder(), query);
            request.setQuery(query);
        } else {
            SearchSourceBuilder searchSourceBuilder = buildSearchBuilder(request);
            SearchPage page = request.getPage();
            if (page != null && page.getEnable()) {
                Integer size = page.getPageSize();
                Integer from = page.getPageNum() != null && page.getPageNum() != 0 ? (page.getPageNum() - 1) * size : 0;
                searchSourceBuilder.from(from).size(size);
            }
            request.setQuery(searchSourceBuilder.toString());
        }
        return getSearch(request);
    }

    public MultiSearch buildSearch(List<SearchRequest> requests) {
        List<Search> searches = Lists.newLinkedList();
        for (SearchRequest request : requests) {
            searches.add(buildSearch(request));
        }
        return new MultiSearch.Builder(searches).build();
    }

    /**
     * 复合搜索
     *      支持主副两次搜索，副搜索的分页，需在SearchRequest中指定，不指定默认为10条。
     * @param requests
     * @return
     */
    public JSONObject search(List<SearchRequest> requests) {
        if (requests.size() == 1) {
            return search(requests.get(0));
        }

        MultiSearch search = buildSearch(requests);
        JestResult result = search(search);
        return convertResult(result, requests.get(0).getPage());
    }

    public JSONObject search(SearchRequest request) {
        JestResult result = search(buildSearch(request));
        return convertResult(result, request.getPage());
    }

    public JSONObject search(String query, List<String> indexName, List<String> indexType, SearchPage page) {
        return search(query, indexName, indexType, page, null);
    }

    public JSONObject search(String query, String indexName, String indexType) {
        return search(query, indexName, indexType, null, null);
    }

    public JSONObject search(String query, String indexName, String indexType, SearchPage page) {
        return search(query, indexName, indexType, page, null);
    }

    public JSONObject search(String query, String indexName, String indexType, SearchPage page, List<SearchOrder> order) {
        return search(query, Lists.newArrayList(indexName), Lists.newArrayList(indexType), page, order);
    }

    public JSONObject search(String query, List<String> indexName, List<String> indexType, SearchPage page, List<SearchOrder> order) {
        SearchRequest request = new SearchRequest();
        request.setQuery(query);
        request.setIndexName(indexName);
        request.setIndexType(indexType);
        request.setPage(page);
        request.setOrder(order);
        JestResult result = search(buildSearch(request));
        return convertResult(result, page);
    }

    public <T extends AbstractAction> JestResult search(T search) {
        JestResult result = null;
        String msg = null;
        try {
            String query = search.getData(new Gson());
            if (query.length() > 4096) {
                query = query.substring(0, 4096) + "...";
            }
            log.info("search [{}]: {}", search.getURI(), query);
            result = clientHandler.execute(search);

            if (result != null && result.isSucceeded()) {
                if (result instanceof MultiSearchResult) {
                    MultiSearchResult searchResult = (MultiSearchResult) result;

                    List<Integer> tookList = searchResult.getResponses().stream().map(c -> c.searchResult.getJsonObject()
                            .get("took").getAsInt()).collect(Collectors.toList());
                    List<Integer> totalList = searchResult.getResponses().stream().map(c -> c.searchResult.getJsonObject()
                            .get(SearchConstants.SEARCH_RESULT_KEY_HITS).getAsJsonObject().
                                    get(SearchConstants.SEARCH_RESULT_KEY_TOTAL).getAsInt()).collect(Collectors.toList());
                    log.info("search result [{}]: took: {}, total: {}", search.getURI(), tookList, totalList);
                } else {
                    JsonObject object = result.getJsonObject();
                    log.info("search result [{}]: {} took: {}, total: {}", search.getURI(), result.getPathToResult(),
                            object.get("took").getAsInt(),
                            object.get(SearchConstants.SEARCH_RESULT_KEY_HITS).getAsJsonObject()
                                    .get(SearchConstants.SEARCH_RESULT_KEY_TOTAL).getAsInt());
                }
                return result;
            }
        } catch (Exception e) {
            msg = e.getMessage();
            if (result != null) {
                msg += ". es msg: " + result.getErrorMessage();
            }
            log.error("search failed! " + msg, e);
        }
        throw new RuntimeException("search failed! " + msg);
    }

    public JSONObject get(String index, String type, String id) {
        try {
            Long begin = System.currentTimeMillis();
            Get get = new Get.Builder(index, id.toString()).type(type).build();
            log.info("get {}/{}/{}", index, type, id);
            JestResult result = clientHandler.execute(get);
            if (result != null && result.isSucceeded()) {
                log.info("get result id: {} time: {}, result: {}", id, System.currentTimeMillis() - begin, result.getSourceAsString());
                return JSON.parseObject(result.getSourceAsString());
            }
        } catch (Exception e) {
            log.error("get failed! ", e);
        }
        log.info("get result: not found document[{}]", id);
        return new JSONObject();
    }

    public List<JSONObject> get(String index, String type, List<?> ids) {
        List<JSONObject> resultList = Lists.newArrayList();
        try {
            Long begin = System.currentTimeMillis();
            List<Doc> docList = Lists.newArrayList();
            ids.forEach(id -> docList.add(new Doc(index, type, id.toString())));

            log.info("multi get {}/{}/{}", index, type, JSONObject.toJSONString(ids));
            if (!docList.isEmpty()) {
                Action action = new MultiGet.Builder.ByDoc(docList).build();
                JestResult result = clientHandler.execute(action);
                if (result != null && result.isSucceeded()) {

                    JSONObject jsonObject = JSON.parseObject(result.getJsonString());
                    JSONArray array = jsonObject.getJSONArray("docs");
                    for (Object obj : array) {
                        JSONObject item = (JSONObject) obj;
                        JSONObject source = item.getJSONObject("_source");
                        if (source != null) {
                            resultList.add(source);
                        }
                    }

                    List<Integer> resultIds = resultList.stream().map(c -> c.getInteger("id")).collect(Collectors.toList());
                    log.info("multi get result time: {}, param ids: {}, result ids: {}", System.currentTimeMillis() - begin,
                            JSONObject.toJSONString(ids), resultIds);
                }
            }
        } catch (Exception e) {
            log.error("multi get failed! ", e);
        }
        return resultList;
    }

    public <T> List<T> searchList(SearchRequest request, Class<T> clazz) {
        JestResult result = search(buildSearch(request));
        List<T> list = Lists.newArrayList();
        if (result != null) {
            if (request.getHighlight() != null && request.getHighlight().getEnable()) {
                Gson gson = buildGsonTypeAdapter();
                Map map = (Map) result.getValue(SearchConstants.SEARCH_RESULT_KEY_HITS);
                List<Map> hitsList = (List<Map>) map.get(SearchConstants.SEARCH_RESULT_KEY_HITS);
                for (Map hit : hitsList) {
                    Map sourceMap = (Map) hit.get(SearchConstants.SEARCH_RESULT_KEY_SOURCE);
                    Map highMap = (Map) hit.get(SearchConstants.SEARCH_RESULT_KEY_HIGHLIGHT);
                    String value = "";
                    String fieldName = request.getHighlight().getHighlightFieldName();
                    List valueList = (List) highMap.get(fieldName);
                    for (Object val : valueList)
                        value += val.toString();
                    if (!"".equals(value))
                        sourceMap.put(fieldName, value);
                    T t = gson.fromJson(mapToJson(sourceMap), clazz);
                    list.add(t);
                }
            } else
                list = result.getSourceAsObjectList(clazz);
        }
        return list;
    }

    public <T> Map<String, Object> searchMap(SearchRequest request, Class<T> clazz) {
        Map<String, Object> map = Maps.newHashMap();
        JestResult result = search(buildSearch(request));
        if (result != null) {
            List<T> list = result.getSourceAsObjectList(clazz);
            map.put(SearchConstants.SEARCH_RESULT_KEY_RESULT, list);
            Map hitsMap = (Map) result.getValue(SearchConstants.SEARCH_RESULT_KEY_HITS);
            Number total = (Number) hitsMap.get(SearchConstants.SEARCH_RESULT_KEY_TOTAL);
            map.put(SearchConstants.SEARCH_RESULT_KEY_TOTAL, total.intValue());
        }
        return map;
    }

    public SearchSourceBuilder buildSearchBuilder(SearchRequest request) {

        if (request.getSearchFieldSize() == 0) {
            throw new RuntimeException("search field can't be null");
        }

        SearchSourceBuilder builder = new SearchSourceBuilder();
        BoolQueryBuilder query = QueryBuilders.boolQuery();

        Object value = request.getValue();


        if (request.getMatchAllFieldSize() > 0) {
            query.must(QueryBuilders.matchAllQuery());
        } else {
            buildQueryField(query, value, request.getMust(), Clause.must);
            buildQueryField(query, value, request.getShould(), Clause.should);
            buildQueryField(query, value, request.getMustNot(), Clause.mustNot);
        }

        if (request.getOrder() != null) {
            for (SearchOrder order : request.getOrder()) {
                if (order.getName() != null) {
                    builder.sort(order.getName(), order.getSort());
                }
            }
        }

        if (request.getHighlight() != null) {
            SearchRequest.Highlight highlight = request.getHighlight();

            if (highlight.getEnable()) {
                HighlightBuilder highlightBuilder = builder.highlighter();
                highlightBuilder.field(highlight.getHighlightFieldName());
                highlightBuilder.preTags(highlight.getPreTag());
                highlightBuilder.postTags(highlight.getPostTag());
            }
        }

        return builder.query(query);
    }

    private void buildQueryField(BoolQueryBuilder query, Object value, List<SearchField> fields, Clause clause) {
        if (fields == null || fields.size() == 0)
            return;

        for (SearchField field : fields) {
            QueryBuilder queryBuilder = null;
            String name = field.getFieldName();
            value = field.getValue() != null ? field.getValue() : value;
            String val = value != null ? value.toString().toLowerCase() : "";
            QueryType queryType = field.getQueryType();

            switch (queryType) {
                case term: queryBuilder = QueryBuilders.termQuery(name, val); break;
                case prefix: queryBuilder = QueryBuilders.prefixQuery(name, val); break;
                case query_string: queryBuilder = QueryBuilders.queryStringQuery(val).defaultField(name); break;
                case match: queryBuilder = QueryBuilders.matchQuery(name, val); break;
                case match_phrase: queryBuilder = QueryBuilders.matchPhraseQuery(name, val); break;
                case multi_match: queryBuilder = multiMatchQuery(name, val); break;
                case wildcard: queryBuilder = QueryBuilders.wildcardQuery(name, val); break;
                case regexp: queryBuilder = QueryBuilders.regexpQuery(name, value.toString()); break;
                case range: queryBuilder = rangeQuery(name, val); break;
                default: QueryBuilders.termQuery(name, val);
            }

            if (Clause.should.equals(clause))
                query.should(queryBuilder);
            else if (Clause.must.equals(clause))
                query.must(queryBuilder);
            else if (Clause.mustNot.equals(clause))
                query.mustNot(queryBuilder);
        }
    }

    private QueryBuilder multiMatchQuery(String name, String val) {
        String names = name;
        if (names.indexOf(",") > 0) {
            return QueryBuilders.multiMatchQuery(val, names.split(","));
        }
        return null;
    }

    private QueryBuilder rangeQuery(String name, String val) {
        String[] range = val.split("#");
        return QueryBuilders.rangeQuery(name).gt(range[0]).lt(range[1]);
    }

    private String buildOrder(List<SearchOrder> order, String query) {
        if (order != null && order.size() > 0) {
            JSONObject json = JSON.parseObject(query);
            JSONArray array = new JSONArray();
            order.stream().forEach(o -> array.add(JSON.parseObject("{\"" + o.getName() + "\":\"" + o.getSort() + "\"}")));
            json.put("sort", array);
            query = json.toJSONString();
        }
        return query;
    }

    private JSONObject convertResult(JestResult result, SearchPage page) {
        JSONObject response = new JSONObject(16, true);

        if (result instanceof MultiSearchResult) {
            MultiSearchResult searchResult = (MultiSearchResult) result;
            searchResult.getResponses().forEach(c -> fullDataResult(c.searchResult, page, response));
        } else {
            fullDataResult(result, page, response);
        }
        return response;
    }

    private void fullDataResult(JestResult result, SearchPage page, JSONObject response) {
        Gson gson = buildGsonTypeAdapter();
        JSONArray array = new JSONArray();

        Map map = (Map) result.getValue(SearchConstants.SEARCH_RESULT_KEY_HITS);
        List<Map> hitsList = (List<Map>) map.get(SearchConstants.SEARCH_RESULT_KEY_HITS);
        if (hitsList != null && hitsList.size() > 0) {
            for (Map hit : hitsList) {
                Map sourceMap = (Map) hit.get(SearchConstants.SEARCH_RESULT_KEY_SOURCE);
                String jsonStr = gson.toJson(sourceMap);
                array.add(JSON.parseObject(jsonStr));
            }
        }

        String dataKey = SearchConstants.RESULT_DATA_KEY;

        if (response.get(dataKey) == null) {
            if (page != null && page.getEnable()) {
                Number total = (Number) map.get(SearchConstants.SEARCH_RESULT_KEY_TOTAL);
                page.setTotalNum(total.intValue());
                response.put(SearchConstants.RESULT_PAGE_KEY, page);
            }
        } else {
            dataKey = SearchConstants.RESULT_DATA_ATT_KEY;
        }

        response.put(dataKey, array);
    }

    private String mapToJson(Map map) {
        StringBuilder json = new StringBuilder();
        if (map != null && map.size() > 0) {
            json.append("{");
            for (Iterator it = map.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry e = (Map.Entry) it.next();
                json.append("\"").append(e.getKey()).append("\":\"").append(e.getValue()).append("\",");
            }
            String val = json.substring(0, json.lastIndexOf(",")) + "}";
            return val;
        }
        return null;
    }

    private Gson buildGsonTypeAdapter() {
        return new GsonBuilder().registerTypeAdapter(Double.class, new JsonSerializer<Double>() {
            @Override
            public JsonElement serialize(Double src, Type typeOfSrc, JsonSerializationContext context) {
                if (src == src.longValue())
                    return new JsonPrimitive(src.longValue());
                return new JsonPrimitive(src);
            }
        }).create();
    }

}
