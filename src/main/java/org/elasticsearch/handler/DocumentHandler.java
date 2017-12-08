package org.elasticsearch.handler;


import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.util.List;

/**
 * Created by Bob Jiang on 2016/10/27.
 */
public class DocumentHandler {

    private ClientHandler clientHandler = ClientHandler.getInstance();

    private volatile static DocumentHandler documentHandler;

    private DocumentHandler() {}

    public static DocumentHandler getInstance() {
        if (documentHandler == null) {
            synchronized (DocumentHandler.class) {
                if (documentHandler == null) {
                    documentHandler = new DocumentHandler();
                }
            }
        }
        return documentHandler;
    }


    public boolean saveDoc(String indexName, String indexType, String indexId, Object data) {
        try {
            Index.Builder builder = new Index.Builder(data).index(indexName);
            if (indexId != null) {
                builder.id(indexId);
            }
            Index index = builder.type(indexType).build();
            JestResult result = clientHandler.execute(index);
            return (result != null && result.isSucceeded()) ? true : false;
        } catch (Exception e) {
            throw new RuntimeException("saveDoc failed! " + e.getMessage(), e);
        }
    }

    public <T> boolean saveDoc(String indexName, String indexType, T data) {
        String docId = getFieldValue(data, "id");
        if (docId != null) {
            return saveDoc(indexName, indexType, docId, data);
        }
        return false;
    }

    public <T> void saveDocList(String indexName, String indexType, List<T> dataList) {
        saveDocList(indexName, indexType, "id", dataList);
    }

    public <T> void saveDocList(String indexName, String indexType, String indexIdField, List<T> dataList) {
        if (dataList == null || dataList.size() == 0) {
            throw new RuntimeException("dataList is null");
        }
        for (T t : dataList) {
            String docId = getFieldValue(t, indexIdField);
            if (docId != null) {
                saveDoc(indexName, indexType, docId, t);
            }
        }
    }

    public <T> boolean bulkSaveDocList(String indexName, String indexType, List<T> dataList) {
        try {
            Bulk.Builder builder = new Bulk.Builder().defaultIndex(indexName).defaultType(indexType);
            List<Index> list = Lists.newArrayList();
            for (T t : dataList) {
                String docId = getFieldValue(t, "id");
                if (docId != null) {
                    list.add(new Index.Builder(t).id(docId).build());
                }
            }
            builder.addAction(list);
            JestResult result = clientHandler.execute(builder.build());
            return (result != null && result.isSucceeded()) ? true : false;
        } catch (Exception e) {
            throw new RuntimeException("bulkSaveDocList failed! " + e.getMessage(), e);
        }
    }

    public boolean deleteDoc(String indexName, String indexType, String indexId) {
        try {
            Delete delete = new Delete.Builder(indexId).index(indexName).type(indexType).build();
            JestResult result = clientHandler.execute(new Bulk.Builder().addAction(delete).build());
            return (result != null && result.isSucceeded()) ? true : false;
        } catch (Exception e) {
            throw new RuntimeException("deleteDoc failed! " + e.getMessage(), e);
        }
    }

    public boolean updateDoc(String indexName, String indexType, String indexId, Object data) {
        try {
            Update update = new Update.Builder(data).index(indexName).type(indexType).id(indexId).build();
            JestResult result = clientHandler.execute(new Bulk.Builder().addAction(update).build());
            return (result != null && result.isSucceeded()) ? true : false;
        } catch (Exception e) {
            throw new RuntimeException("deleteDoc failed! " + e.getMessage(), e);
        }
    }

    public boolean deleteByQuery(String indexName, String indexType, String query) {
        try {
            DeleteByQuery deleteByQuery = new DeleteByQuery.Builder(query).addIndex(indexName).addType(indexType).build();
            JestResult result = clientHandler.execute(deleteByQuery);
            return (result != null && result.isSucceeded()) ? true : false;
        } catch (IOException e) {
            throw new RuntimeException("deleteDoc failed! " + e.getMessage(), e);
        }
    }

    private String getFieldValue(Object obj, String fieldName) {
        try {
            if (obj instanceof JSONObject) {
                return ((JSONObject) obj).get("id").toString();
            } else {
                PropertyDescriptor pd = new PropertyDescriptor(fieldName, obj.getClass());
                return pd.getReadMethod().invoke(obj).toString();
            }
        } catch (Exception e) {
            return null;
//            throw new RuntimeException("fieldName[" + fieldName + "] was wrong", e);
        }
    }

}
