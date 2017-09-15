package org.elasticsearch.handler.dto;

import org.elasticsearch.search.sort.SortOrder;

import java.io.Serializable;

/**
 * Created by Bob Jiang on 2017/8/2.
 */
public class SearchOrder implements Serializable {

    private static final long serialVersionUID = -8729814906697428165L;

    //排序字段名称
    private String name;
    //顺序
    private SortOrder sort = SortOrder.ASC;

    public SearchOrder() {}

    public SearchOrder(String name) {
        this.name = name;
    }

    public SearchOrder(String name, SortOrder sort) {
        this.name = name;
        this.sort = sort;
    }

    public SearchOrder(String name, String sort) {
        this.name = name;
        if (SortOrder.ASC.toString().equals(sort)) {
            this.sort = SortOrder.ASC;
        } else if (SortOrder.DESC.toString().equals(sort)) {
            this.sort = SortOrder.DESC;
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SortOrder getSort() {
        return sort;
    }

    public void setSort(SortOrder sort) {
        this.sort = sort;
    }

    @Override
    public String toString() {
        return "Order{" +
                "name='" + name + '\'' +
                ", sort=" + sort +
                '}';
    }
}
