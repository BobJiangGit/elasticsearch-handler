package org.elasticsearch.handler.constants;

/**
 * Created by Bob Jiang on 2016/10/27.
 */
public interface SearchConstants {

    String PAGE_NUM = "pageNum";

    String PAGE_SIZE = "pageSize";

    int SEARCH_PAGE_SIZE = 20;

    int SEARCH_DEFAULT_SIZE = 2000;

    int SEARCH_MATCH_SIZE = 200;

    boolean SEARCH_PAGE_ENABLE = false;

    String SEARCH_RESULT_KEY_HITS = "hits";

    String SEARCH_RESULT_KEY_TOTAL = "total";

    String SEARCH_RESULT_KEY_SOURCE = "_source";

    String SEARCH_RESULT_KEY_HIGHLIGHT = "highlight";

    String SEARCH_RESULT_KEY_RESULT = "result";

    boolean SEARCH_HIGHLIGHT_ENABLE = false;

    String HIGHLIGHT_PRE_TAG = "<b>";

    String HIGHLIGHT_POST_TAG = "</b>";

    String RESULT_DATA_KEY = "data";

    String RESULT_PAGE_KEY = "page";
}
