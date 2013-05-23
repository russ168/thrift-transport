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
public class GenerateNameTests {

    @BeforeMethod
    public void setupIndexWriterSearcher() throws Exception, TTransportException {
    }

    @AfterMethod
    public void close() throws IOException {
    }

    @Test
    public void testGenerateName() throws  Exception {
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("test");
        DBCollection foo = db.getCollection("foo");
        BasicDBObject doc = new BasicDBObject("name", "jackie")
                                            .append("password", "123")
                                            .append("email", "jackiedong168@gmail.com");
        foo.insert(doc);
        mongoClient.close();

    }

    @Test
    public void testGenerateNames() throws Exception {
        for(int i = 0; i < 10; i++) {
            System.out.println(generateRandomString(5, Mode.ALPHANUMBERIC));
            //System.out.println("\n");
        }
    }

    public static enum Mode {
        ALPHA, ALPHANUMBERIC, NUMBERIC
    }

    public static String generateRandomString(int length, Mode mode) throws Exception {
        StringBuffer buffer = new StringBuffer();
        String characters = "";

        switch (mode) {
            case ALPHA:
                characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
                break;

            case ALPHANUMBERIC:
                characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
                break;

            case NUMBERIC:
                characters = "1234567890";
                break;
        }

        int charactersLength = characters.length();

        for (int i = 0; i < length; i++) {
            double index = Math.random() * charactersLength;
            buffer.append(characters.charAt((int)index));
        }

        return buffer.toString();

    }







}