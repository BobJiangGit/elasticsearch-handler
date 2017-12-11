package org.elasticsearch.handler;


import io.searchbox.client.JestResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.strings.StringUtils;


/**
 * Created by Bob Jiang on 2016/10/27.
 */
public class IndexHandler {

    private volatile static IndexHandler indexHandler;

    private ClientHandler clientHandler = ClientHandler.getInstance();

    private IndexHandler() {}

    public static IndexHandler getInstance() {
        if (indexHandler == null) {
            synchronized (IndexHandler.class) {
                if (indexHandler == null) {
                    indexHandler = new IndexHandler();
                }
            }
        }
        return indexHandler;
    }

    public boolean createIndex(String indexName) {
        return createIndex(indexName, null, null, null);
    }

    public boolean createIndex(String indexName, String settings) {
        return createIndex(indexName, null, settings, null);
    }

    public boolean createIndex(String indexName, String indexType, String mappings) {
        return createIndex(indexName, indexType, null, mappings);
    }

    public boolean createIndex(String indexName, String indexType, String settings, String mappings) {
        try {
            CreateIndex.Builder builder = new CreateIndex.Builder(indexName);
            if (!StringUtils.isBlank(settings)) {
                builder.settings(settings);
            }
            JestResult result = clientHandler.execute(builder.build());
            if (result != null && result.isSucceeded()) {
                if (mappings != null) {
                    return createMapping(indexName, indexType, mappings);
                }
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException("createIndex failed! " + e.getMessage(), e);
        }
    }

    public boolean existsIndex(String indexName) {
        try {
            JestResult result = clientHandler.execute(new IndicesExists.Builder(indexName).build());
            return (result != null && result.isSucceeded()) ? true : false;
        } catch (Exception e) {
            throw new RuntimeException("existsIndex failed! " + e.getMessage(), e);
        }
    }

    public boolean deleteIndex(String indexName) {
        try {
            JestResult result = clientHandler.execute(new DeleteIndex.Builder(indexName).build());
            return (result != null && result.isSucceeded()) ? true : false;
        } catch (Exception e) {
            throw new RuntimeException("deleteIndex failed! " + e.getMessage(), e);
        }
    }

    public boolean createMapping(String indexName, String indexType, String sourceJson) {
        try {
            JestResult result = clientHandler.execute(new PutMapping.Builder(indexName, indexType, sourceJson).build());
            return (result != null && result.isSucceeded()) ? true : false;
        } catch (Exception e) {
            throw new RuntimeException("createMapping failed! " + e.getMessage(), e);
        }
    }

}
