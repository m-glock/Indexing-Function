package azure.function.indexing;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.util.NamedList;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;

public class IndexHandler {

    private HttpSolrClient client;
    private HashMap<String, JSONObject> fileMetadata;
    private Logger logger;

    public IndexHandler(String solrURL, String collectionName, Logger logger){
        this.logger = logger;
        client = new HttpSolrClient.Builder(solrURL).build();
        client.setBaseURL(client.getBaseURL() + "/" + collectionName);
    }

    public void indexFiles(String folderPath, HashMap<String, JSONObject> fileMetadata) throws IOException, SolrServerException, NullPointerException {
        logger.info("Starting to build indexing requests...");
        this.fileMetadata = fileMetadata;
        File folder = new File(folderPath);
        logger.info("before adding files to solr. Folder is " + folder.isFile());
        addFilesToSolr(folder);
        client.commit();
    }

    private void addFilesToSolr(final File folder) throws IOException, SolrServerException, NullPointerException{
        File[] listOfFiles;
        logger.info("Preparing request for " + folder.getPath());
        if(folder.isDirectory()){
            listOfFiles = folder.listFiles();
        }else {
            listOfFiles = new File[]{folder};
        }
        for (File file : listOfFiles) {
            if (file.isFile()) {
                logger.info("Building indexing request for file " + file.getName());
                ContentStreamUpdateRequest request = new ContentStreamUpdateRequest("/update/extract");
                FileStream fileInputStream = null;
                try {
                    fileInputStream = new FileStream(file.getName(), file.length(), file.toURI().toString(), file);
                    request.addContentStream(fileInputStream);
                    request = setParameters(request, file);
                    logger.info("Send request.");
                    NamedList<Object> response = client.request(request);
                    System.out.println(response);
                } finally {
                    if (fileInputStream != null) {
                        fileInputStream.close();
                    }
                }
            }else if(file.isDirectory()){
                addFilesToSolr(file);
            }
        }
    }

    private ContentStreamUpdateRequest setParameters(ContentStreamUpdateRequest request, File file){
        String fileName = file.getName();
        request.setParam("literal.stream_name", fileName);
        if (fileMetadata.isEmpty()){
            request.setParam("literal.path", file.toString());
            request.setParam("literal.owner", System.getProperty("user.name"));
        }else{
            try {
                JSONObject fileData = fileMetadata.get(fileName);
                if(fileData != null){
                    request.setParam("literal.path", fileData.get("fileURL").toString());
                    request.setParam("literal.owner", fileData.get("fileCreator").toString());
                }else{
                    logger.info(fileName + " has no metadata");
                }
            }catch(JSONException ex){
                logger.info("Exception when accessing additional Data from the SharePoint files.");
            }
        }
        return request;
    }

    public String deleteFileFromIndex(String folderPath) throws SolrServerException, IOException{
        folderPath = folderPath.replace("\\","/");
        String query = "path:\"" + folderPath + "\"";
        logger.info("DELETING " + folderPath + "FROM THE INDEX");
        UpdateResponse resp = client.deleteByQuery(query, 1000);
        return resp.getResponse().toString();
    }
}
