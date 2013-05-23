package org.elasticsearch.thrift.test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import org.apache.thrift.transport.TTransportException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * User: Dong ai hua
 * Date: 13-4-19
 * Time: 下午5:16
 * To change this template use File | Settings | File Templates.
 */
public class MongodbTests {

    @BeforeMethod
    public void setupIndexWriterSearcher() throws Exception, TTransportException {
    }

    @AfterMethod
    public void close() throws IOException {
    }

    @Test
    public void testCreateCollection() throws  Exception {
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("test");
        DBCollection foo = db.getCollection("foo");
        BasicDBObject doc = new BasicDBObject("name", "jackie")
                                            .append("password", "123")
                                            .append("email", "jackiedong168@gmail.com");
        foo.insert(doc);
        mongoClient.close();

    }
}