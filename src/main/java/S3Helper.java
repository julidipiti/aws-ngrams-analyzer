import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Helper to interact with S3 on AWS.
 *
 * @author julidipiti
 */
public class S3Helper {

  private static AmazonS3 s3;
  private static final String emrBucket = "datasets.elasticmapreduce";
  private static final String ngramsPath = "ngrams/books/20090715/";

  public S3Helper(AWSCredentials credentials, Region region) {
    s3 = new AmazonS3Client(credentials);
    s3.setRegion(region);
  }

  /**
   * Creates a bucket in S3.
   *
   * @param bucketName The name of the bucket, which must be unique.
   */
  void createBucket(String bucketName) {
    try {
      IOHelper.println();
      IOHelper.println("Creating a bucket with name: " + bucketName);
      s3.createBucket(bucketName);
    } catch (AmazonServiceException ase) {
      IOHelper.println(
          "Caught an AmazonServiceException, which means your request made it to Amazon S3, but was"
              + " rejected with an error response for some reason.");
      IOHelper.println("Error Message:    " + ase.getMessage());
      IOHelper.println("HTTP Status Code: " + ase.getStatusCode());
      IOHelper.println("AWS Error Code:   " + ase.getErrorCode());
      IOHelper.println("Error Type:       " + ase.getErrorType());
      IOHelper.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      IOHelper.println(
          "Caught an AmazonClientException, which means the client encountered a serious internal "
          + "problem while trying to communicate with S3, such as not being able to access the "
          + "network.");
      IOHelper.println("Error Message: " + ace.getMessage());
    }
  }

  /**
   * Inserts an object on the specified bucket, in S3.
   *
   * @param bucketName The existing bucket where to insert the file.
   * @param relativePath The path within the bucket to insert the new object.
   * @param file The file to insert in S3, which is going to be an object in the file system of S3.
   */
  void putObject(String bucketName, String relativePath, File file) {
    try {
      IOHelper.println();
      IOHelper.println("Uploading file " + file.getName() + " ...");
      IOHelper.println("Path on S3: s3://" + bucketName + "/" + relativePath);
      s3.putObject(new PutObjectRequest(bucketName, relativePath + file.getName(), file));
    } catch (AmazonServiceException ase) {
      IOHelper.println(
          "Caught an AmazonServiceException, which means your request made it to Amazon S3, but was"
          + " rejected with an error response for some reason.");
      IOHelper.println("Error Message:    " + ase.getMessage());
      IOHelper.println("HTTP Status Code: " + ase.getStatusCode());
      IOHelper.println("AWS Error Code:   " + ase.getErrorCode());
      IOHelper.println("Error Type:       " + ase.getErrorType());
      IOHelper.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      IOHelper.println(
          "Caught an AmazonClientException, which means the client encountered a serious internal "
          + "problem while trying to communicate with S3, such as not being able to access the "
          + "network.");
      IOHelper.println("Error Message: " + ace.getMessage());
    }
  }

  /**
   * Gets the files from the resources within the project and uploads them in S3 to be available for
   * later use.
   */
  void uploadHiveScripts(String bucketName, String scriptsRelativePath) throws IOException {

    File file =
        streamToFile(
            Main.class.getResourceAsStream("/hiveScripts/ImportNgrams.q"), "ImportNgrams.q");
    putObject(bucketName, scriptsRelativePath, file);

    file =
        streamToFile(
            Main.class.getResourceAsStream("/hiveScripts/CreateWindow.q"), "CreateWindow.q");
    putObject(bucketName, scriptsRelativePath, file);

    file =
        streamToFile(Main.class.getResourceAsStream("/hiveScripts/ShiftWindow.q"), "ShiftWindow.q");
    putObject(bucketName, scriptsRelativePath, file);

    file =
        streamToFile(
            Main.class.getResourceAsStream("/hiveScripts/ExportDictionary.q"),
            "ExportDictionary.q");
    putObject(bucketName, scriptsRelativePath, file);

    file =
        streamToFile(
            Main.class.getResourceAsStream("/hiveScripts/ExportForeignisms.q"),
            "ExportForeignisms.q");
    putObject(bucketName, scriptsRelativePath, file);

    file =
        streamToFile(
            Main.class.getResourceAsStream("/hiveScripts/ProcessNeologisms.q"),
            "ProcessNeologisms.q");
    putObject(bucketName, scriptsRelativePath, file);

    file =
        streamToFile(
            Main.class.getResourceAsStream("/hiveScripts/ExportNeologisms.q"),
            "ExportNeologisms.q");
    putObject(bucketName, scriptsRelativePath, file);
  }

  /**
   * Creates a file from a stream. This method is needed because the files in the program are loaded
   * as resources, which we get a streams. Then on execution we create temporary files to upload to
   * S3 and then be deleted when the VM stops.
   *
   * @param in The stream with the script.
   * @param name The name of the file to create.
   * @return A file with the content of the stream.
   * @throws IOException An exception that indicates some problem in the execution.
   */
  private File streamToFile(InputStream in, String name) throws IOException {
    final File tmp = new File(name);
    tmp.deleteOnExit();
    try (FileOutputStream out = new FileOutputStream(tmp)) {
      IOUtils.copy(in, out);
    }
    return tmp;
  }

  /**
   * Gets the ngram option from which to extract data.
   *
   * @return A string representing the ngram language selected.
   */
  public String selectLanguageOption() {
    IOHelper.println("Loading options...");
    ObjectListing ol =
        s3.listObjects(
            new ListObjectsRequest()
                .withBucketName(emrBucket)
                .withPrefix(ngramsPath)
                .withDelimiter("/"));

    IOHelper.println("Language options:");

    List<String> languagePaths = ol.getCommonPrefixes();
    Iterator<String> it = languagePaths.iterator();
    String[] languages = new String[languagePaths.size()];
    for (int i = 0; i < languagePaths.size(); i++) {
      languages[i] = getLanguageName(it.next());
      IOHelper.println("\t" + (i + 1) + ". " + languages[i]);
    }

    IOHelper.println();
    IOHelper.println("Insert number option:");
    int opt = IOHelper.getInteger();
    if (opt < 1 || opt > languages.length) {
      throw new IllegalArgumentException("Incorrect option.");
    }

    return languages[opt - 1];
  }

  /**
   * Removes the path and last slash from the string.
   *
   * @param s The string to get the name of the language from.
   * @return The name of the ngrams language, i.e.: eng-all.
   */
  private String getLanguageName(String s) {
    return s.substring(ngramsPath.length(), s.length() - 1);
  }
}
