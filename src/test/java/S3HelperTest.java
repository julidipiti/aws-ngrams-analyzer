import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;


public class S3HelperTest {

  private static AmazonS3 s3;
  private S3Helper s3h;

  /**
   * Gets the environment ready for testing by setting the AWSCredentials (can not be mocked for
   * some of the tests) and setting the region.
   */
  @Before
  public void setUp() {
    AWSCredentials credentials = new ProfileCredentialsProvider("default").getCredentials();
    Region region = Region.getRegion(Regions.US_EAST_1);
    s3h = new S3Helper(credentials, region);
    s3 = new AmazonS3Client(credentials);
  }

  /**
   * Tests that a bucket is created and exists in S3. Then deletes it for cleaning purposes.
   *
   * @throws InterruptedException if there is any interruption while sleeping the thread.
   */
  @Test
  public void createBucketTest() throws InterruptedException {
    String bucketName = "ana-test-" + UUID.randomUUID();
    assertFalse(s3.doesBucketExist(bucketName));
    s3h.createBucket(bucketName);
    // Non-fancy solution until the Java SDK includes some waiter methods,
    // just as in the PHP version and its waitUntil methods.
    Thread.sleep(6000);
    assertTrue(s3.doesBucketExist(bucketName));
    // Clean up
    s3.deleteBucket(bucketName);
  }

  /**
   * Tests that an object can be put in an existing bucket in S3. Then deletes the object and the
   * bucket for cleaning purposes.
   *
   * @throws IOException if the temp file can not be created.
   */
  @Test
  public void putObjectTest() throws IOException {
    String bucketName = "ana-test-" + UUID.randomUUID();
    s3h.createBucket(bucketName);

    String fileName = "ana-test-file";
    String fileExt = ".tmp";
    File temp = File.createTempFile(fileName, fileExt);
    assertFalse(s3.doesObjectExist(bucketName, temp.getName()));
    s3h.putObject(bucketName, "", temp);
    assertTrue(s3.doesObjectExist(bucketName, temp.getName()));
    // Clean up
    s3.deleteObject(bucketName, temp.getName());
    s3.deleteBucket(bucketName);
    temp.delete();
  }
}
