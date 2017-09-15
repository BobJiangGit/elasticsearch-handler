package org.elasticsearch.handler.prop;


import java.util.List;

/**
 * Created by Bob Jiang on 2016/10/27.
 */
public class SearchProperty {

    private List<String> hosts;
    private Integer readTimeout;
    private Integer connTimeout;

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public Integer getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Integer readTimeout) {
        this.readTimeout = readTimeout;
    }

    public Integer getConnTimeout() {
        return connTimeout;
    }

    public void setConnTimeout(Integer connTimeout) {
        this.connTimeout = connTimeout;
    }

    @Override
    public String toString() {
        return "SearchProperty{" +
                "hosts=" + hosts +
                ", readTimeout=" + readTimeout +
                ", connTimeout=" + connTimeout +
                '}';
    }
}
