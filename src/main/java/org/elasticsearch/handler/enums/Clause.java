package org.elasticsearch.handler.enums;

/**
 * 匹配条件
 *
 * Created by Bob Jiang on 2016/10/27.
 */
public enum Clause {
    /**
     * 必须完全匹配
     */
    must,
    /**
     * 至少匹配一个条件
     */
    should,
    /**
     * 必须不不匹配条件
     */
    mustNot;
}
