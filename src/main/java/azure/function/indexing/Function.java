package azure.function.indexing;

import java.util.*;
import java.util.logging.Logger;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
    @FunctionName("IndexFilesToSolr")
    public HttpResponseMessage run(
            @HttpTrigger(name = "test", methods = {HttpMethod.GET, HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS) HttpRequestMessage<Optional<String>> request,
        final ExecutionContext context) {
        Logger logger = context.getLogger();
        logger.info("Java HTTP trigger processed a request.");

        // Parse query parameter
        String parameter = request.getQueryParameters().get("name");

        HttpMethod method = request.getHttpMethod();
        String body = request.getBody().orElse("{}");

        logger.info("Parameter is: " + parameter + " and body is: " + body);
        
        //post request -> index files
        //get request -> delete file
        try {
            JSONObject fullJson = new JSONObject(body);
            String collectionName = "";
            if(fullJson.has("collectionName")){
                collectionName = (String) fullJson.get("collectionName");
            }else{
                collectionName = request.getHeaders().get("collectionname");
            }
            logger.info("Collection name is " + collectionName);
            IndexHandler handler = new IndexHandler("http://localhost:8983/solr", collectionName, logger);

            if (method.equals(HttpMethod.POST)) {
                context.getLogger().info("folder path to be indexed is: " + parameter);
                HashMap<String, JSONObject> fileMetadata = new HashMap<>();
                if(fullJson.has("files")){
                    logger.info("Request contains information about file metadata.");
                    JSONArray arr = fullJson.getJSONArray("files");

                    for (int i = 0; i < arr.length(); i++){
                        JSONObject obj = arr.getJSONObject(i);
                        fileMetadata.put(obj.getString("key"), obj.getJSONObject("value"));
                    }
                }
                handler.indexFiles(parameter, fileMetadata);
                return request.createResponseBuilder(HttpStatus.OK).body("Successfully indexed " + parameter).build();

            }else if(method.equals(HttpMethod.GET)){
                logger.info("Folder path to be deleted is: " + parameter);
                String response = handler.deleteFileFromIndex(parameter);
                return request.createResponseBuilder(HttpStatus.OK).body("Successfully deleted " + parameter + ". Response: " + response).build();

            }else{
                return request.createResponseBuilder(HttpStatus.METHOD_NOT_ALLOWED).body(method + " requests are not accepted by this function.").build();
            }
        }catch(Exception ex){
            return request.createResponseBuilder(HttpStatus.OK).body("Error from the indexing function. " + ex.getClass().getName() + ": " + ex.getMessage()).build();
        }


    }
}
