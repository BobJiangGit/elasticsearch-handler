package org.elasticsearch.handler.enums;

/**
 * 查询方式
 *
 * Created by Bob Jiang on 2016/11/1.
 */
public enum QueryType {

    /**
     * 精确匹配
     */
    term,
    /**
     * 词组匹配
     */
    match,
    /**
     * 短语匹配
     */
    match_phrase,
    /**
     * 多字段词组匹配
     */
    multi_match,
    /**
     * 条件匹配
     */
    query_string,
    /**
     * 前缀匹配
     */
    prefix,
    /**
     * 匹配所有
     */
    match_all,
    /**
     * 通配符
     */
    wildcard,
    /**
     * 正则表达式
     */
    regexp,
    /**
     * 范围匹配
     */
    range;
}
