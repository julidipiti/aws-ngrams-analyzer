import static org.junit.Assert.assertEquals;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


public class EMRHelperTest {

  private EMRHelper emrh;

  /**
   * Gets the environment ready for testing by mocking the AWSCredentials and setting the region.
   */
  @Before
  public void setUp() {
    AWSCredentials fakeCredentials = Mockito.mock(AWSCredentials.class);
    Region region = Region.getRegion(Regions.US_EAST_1);
    emrh = new EMRHelper(fakeCredentials, region);
  }

  /**
   * Tests that creating a StepConfig through the helper is the same as doing it through the
   * stepFactory.
   */
  @Test
  public void getHiveStepTest() {
    String[] args = {"1", "two", "III"};
    String scriptPath = "script path";
    String name = "name";

    StepConfig stepConfig = emrh.getHiveStep(name, scriptPath, args);
    HadoopJarStepConfig hjsc = new StepFactory().newRunHiveScriptStep(scriptPath, args);

    assertEquals(stepConfig.getName(), name);
    assertEquals(hjsc, stepConfig.getHadoopJarStep());
  }

  /**
   * Tests that creating a JobFlowInstancesConfig through the helper is the same as doing it through
   * the JobFlowInstancesConfig constructor.
   */
  @Test
  public void getJobFlowInstancesConfigTest() {
    int instanceCount = 5;
    String masterInstanceType = "m4.large";
    String slaveInstanceType = "m3.xlarge";
    JobFlowInstancesConfig jfic1 =
        new JobFlowInstancesConfig()
            .withInstanceCount(instanceCount)
            .withMasterInstanceType(masterInstanceType)
            .withSlaveInstanceType(slaveInstanceType);

    JobFlowInstancesConfig jfic2 =
        emrh.getJobFlowInstancesConfig(instanceCount, masterInstanceType, slaveInstanceType);

    assertEquals(jfic1.getInstanceCount(), jfic2.getInstanceCount());
    assertEquals(jfic1.getMasterInstanceType(), jfic2.getMasterInstanceType());
    assertEquals(jfic1.getSlaveInstanceType(), jfic2.getSlaveInstanceType());
  }

  /**
   * Tests that creating a RunJobFlowRequest through the helper is the same as doing it though the
   * RunJobFlowRequest constructor.
   */
  @Test
  public void getRunJobFlowRequestTest() {
    Application[] apps = {new Application().withName("hive"), new Application().withName("hadoop")};

    Configuration[] configs = {
      new Configuration().withClassification("hive-site"),
      new Configuration().withClassification("mapred-site")
    };

    List<StepConfig> steps = new LinkedList<StepConfig>();
    steps.add(new StepConfig().withName("Step 1"));
    steps.add(
        new StepConfig().withName("Step 2").withActionOnFailure(ActionOnFailure.TERMINATE_CLUSTER));
    steps.add(
        new StepConfig()
            .withName("Step 3")
            .withActionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW));

    JobFlowInstancesConfig jfic =
        new JobFlowInstancesConfig()
            .withInstanceCount(3)
            .withMasterInstanceType("c3.xlarge")
            .withSlaveInstanceType("m1.xlarge");

    String logsPath = "my/logs/path";

    RunJobFlowRequest rjfr1 =
        new RunJobFlowRequest()
            .withApplications(apps)
            .withConfigurations(configs)
            .withSteps(steps)
            .withLogUri(logsPath)
            .withInstances(jfic);

    RunJobFlowRequest rjfr2 = emrh.getRunJobFlowRequest(apps, configs, steps, jfic, logsPath);

    assertEquals(rjfr1.getApplications(), rjfr2.getApplications());
    assertEquals(rjfr1.getConfigurations(), rjfr2.getConfigurations());
    assertEquals(rjfr1.getSteps(), rjfr2.getSteps());
    assertEquals(rjfr1.getInstances(), rjfr2.getInstances());
    assertEquals(rjfr1.getLogUri(), rjfr2.getLogUri());
  }

  /**
   * Tests that selectInstanceType helps to select an option from a list of instance types,
   * simulating the input from a String.
   * There is a tricky behavior in the input, every time it is accessed it needs to be reset,
   * otherwise the next read skips all the content. So before every access it gets reset and skips
   * the offset which is the already read bytes.
   *
   * @throws UnsupportedEncodingException String.getBytes call can cause an exception if the charset
   *     is not supported.
   */
  @Test
  public void selectInstanceTypeTest() throws UnsupportedEncodingException {
    InstanceType[] enumTypes = InstanceType.values();
    String[] types = new String[enumTypes.length];
    for (int i = 0; i < enumTypes.length; i++) {
      types[i] = enumTypes[i].toString();
    }
    Arrays.sort(types);

    int opt1 = 0;
    int opt2 = 8;
    int opt3 = 20;
    int opt4 = 33;

    String s1 = Integer.toString(1 + opt1) + System.lineSeparator();
    String s2 = Integer.toString(1 + opt2) + System.lineSeparator();
    String s3 = Integer.toString(1 + opt3) + System.lineSeparator();
    String s4 = Integer.toString(1 + opt4) + System.lineSeparator();

    ByteArrayInputStream in1 = new ByteArrayInputStream((s1 + s2 + s3 + s4).getBytes("UTF-8"));
    InputStream stdIn = System.in;
    System.setIn(in1);

    String instanceType1 = emrh.selectInstanceType();
    assertEquals(instanceType1, types[opt1]);

    int offset = s1.getBytes().length;
    in1.reset();
    in1.skip(offset);
    String instanceType2 = emrh.selectInstanceType();
    assertEquals(instanceType2, types[opt2]);

    in1.reset();
    in1.skip(offset += s2.getBytes().length);
    String instanceType3 = emrh.selectInstanceType();
    assertEquals(instanceType3, types[opt3]);

    in1.reset();
    in1.skip(offset += s3.getBytes().length);
    String instanceType4 = emrh.selectInstanceType();
    assertEquals(instanceType4, types[opt4]);

    System.setIn(stdIn);
  }
}
