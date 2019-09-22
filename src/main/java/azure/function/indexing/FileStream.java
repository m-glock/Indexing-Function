package azure.function.indexing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import org.apache.solr.common.util.*;

public class FileStream extends ContentStreamBase { 
    private final FileInputStream fileInputStream; 
    private final File file;
  
    public FileStream(String name, Long size, String sourceInfo, final File f) throws IOException { 
        this.file = f;
        this.fileInputStream = new FileInputStream(this.file);
        this.name = name; 
        this.size = size; 
        this.sourceInfo = sourceInfo; 
    } 
  
    @Override 
    public Reader getReader() throws IOException { 
      final String charset = getCharsetFromContentType(this.contentType); 
      return charset == null ? new InputStreamReader(this.fileInputStream) 
        : new InputStreamReader(this.fileInputStream, charset); 
    }
  
    public InputStream getStream() {
      return this.fileInputStream; 
    } 

    public void close() throws IOException {
        fileInputStream.close();
    }
  } 