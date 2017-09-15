package org.elasticsearch.handler;

import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import org.elasticsearch.handler.prop.SearchProperty;

import java.io.IOException;

/**
 * Created by Bob Jiang on 2016/10/28.
 */
public class ClientHandler {

    private volatile static ClientHandler clientHandler;

    private ClientHandler() {}

    public static ClientHandler getInstance() {
        if (clientHandler == null) {
            synchronized (ClientHandler.class) {
                if (clientHandler == null) {
                    clientHandler = new ClientHandler();
                }
            }
        }
        return clientHandler;
    }

    private SearchProperty searchProperty;
    private JestClient jestClient;

    public JestClient getJestClient() {
        if (jestClient == null) {
            if (searchProperty == null) {
                throw new RuntimeException("searchProperty can't be null");
            }

            HttpClientConfig clientConfig = new HttpClientConfig.Builder(searchProperty.getHosts())
                    .readTimeout(searchProperty.getReadTimeout())
                    .connTimeout(searchProperty.getReadTimeout())
                    .multiThreaded(true).build();

            JestClientFactory factory = new JestClientFactory();
            factory.setHttpClientConfig(clientConfig);
            jestClient = factory.getObject();
        }
        return jestClient;
    }

    public JestResult execute(Action action) throws IOException {
        jestClient = getJestClient();
        return jestClient.execute(action);
    }

    public void closeJestClient() {
        if (jestClient != null) {
            jestClient.shutdownClient();
            jestClient = null;
        }
    }

    public void setSearchProperty(SearchProperty searchProperty) {
        this.searchProperty = searchProperty;
    }
}
