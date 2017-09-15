package org.elasticsearch.handler.dto;


import org.elasticsearch.handler.constants.SearchConstants;

import java.io.Serializable;

/**
 * Created by Bob Jiang on 2017/8/2.
 */
public class SearchPage implements Serializable {

    private static final long serialVersionUID = -1965007570517028016L;

    //是否分页 默认不分页
    private boolean enable = SearchConstants.SEARCH_PAGE_ENABLE;
    //页码
    private Integer pageNum;
    //每页显示数量
    private Integer pageSize;
    //总行数
    private Integer totalNum;
    //总页数
    private Integer totalPage;

    public SearchPage() {}

    public SearchPage(Integer pageNum, Integer pageSize) {
        this.pageNum = pageNum;
        setPageSize(pageSize);
        this.enable = true;
    }

    public boolean getEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public Integer getPageNum() {
        return pageNum;
    }

    public void setPageNum(Integer pageNum) {
        if (pageNum != null && pageNum.intValue() > 0)
            this.pageNum = pageNum;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        if (pageSize != null && pageSize.intValue() > 0)
            this.pageSize = pageSize;
        else
            this.pageSize = SearchConstants.SEARCH_PAGE_SIZE;
    }

    public Integer getTotalNum() {
        return totalNum;
    }

    public void setTotalNum(Integer totalNum) {
        this.totalNum = totalNum;
    }

    public Integer getTotalPage() {
        this.totalPage = totalNum % pageSize == 0 ? totalNum / pageSize : totalNum / pageSize + 1;
        return totalPage;
    }

    public void setTotalPage(Integer totalPage) {
        this.totalPage = totalPage;
    }

    @Override
    public String toString() {
        return "SearchPage{" +
                "enable=" + enable +
                ", pageNum=" + pageNum +
                ", pageSize=" + pageSize +
                ", totalNum=" + totalNum +
                ", totalPage=" + totalPage +
                '}';
    }
}
