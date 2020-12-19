/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package local;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import java.util.HashMap;

import lambda.Transform;
import lambda.Load;
import lambda.Query;


public class Local {
    
    // int main enables testing function from cmd line
    public static void main(String[] args) {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };

        //
        // Test Transform function
        //

        // Transform transform = new Transform();

        // HashMap<String, Object> requestTransform = new HashMap<String, Object>();
        // requestTransform.put("bucketName", "openwhisk.tlq");
        // requestTransform.put("fileName", "100_Sales_Records.csv");

        // HashMap<String, Object> responseTransform = transform.handleRequest(requestTransform, c);
        
        // System.out.println("function response: ");
        // System.out.println(responseTransform.toString());

        //
        // Test Load function
        //

        Load load = new Load();

        HashMap<String, Object> requestLoad = new HashMap<String, Object>();
        requestLoad.put("bucketName", "openwhisk.tlq");
        requestLoad.put("fileName", "Processed_100_Sales_Records.csv");
        requestLoad.put("dbLink", "localhost");
        requestLoad.put("dbName", "562tlq");
        requestLoad.put("username", "root");
        requestLoad.put("password", "password");
        requestLoad.put("tableName", "562_test");

        HashMap<String, Object> responseLoad = load.handleRequest(requestLoad, c);
        
        System.out.println("function response: ");
        System.out.println(responseLoad.toString());

        //
        // Test Query function
        //

        // Query query = new Query();

        // HashMap<String, Object> requestQuery = new HashMap<String, Object>();
        // requestQuery.put("dbLink", "localhost");
        // requestQuery.put("dbName", "562tlq");
        // requestQuery.put("username", "root");
        // requestQuery.put("password", "password");
        // requestQuery.put("tableName", "562_test");
        // // requestQuery.put("Region", "Asia");
        // // requestQuery.put("Country", "Malaysia");
        // // requestQuery.put("Item Type", "Supplies");
        // // requestQuery.put("Sales Channel", "Online");
        // // requestQuery.put("Order Priority", "Medium");

        // HashMap<String, Object> responseQuery = query.handleRequest(requestQuery, c);
        
        // System.out.println("function response: ");
        // System.out.println(responseQuery.toString());
    }
}
