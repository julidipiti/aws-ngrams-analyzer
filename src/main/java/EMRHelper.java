import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Helper to interact with ElasticMapReduce on AWS.
 *
 * @author julidipiti
 */
public class EMRHelper {

  private final AmazonElasticMapReduceClient emr;

  public EMRHelper(AWSCredentials credentials, Region region) {
    this.emr = new AmazonElasticMapReduceClient(credentials);
    this.emr.setRegion(region);
  }

  /**
   * Gets the configurations needed to split and process the hive files properly.
   *
   * @return An array with the configurations described above.
   */
  public Configuration[] getConfigurations() {
    List<Configuration> configurations = new LinkedList<>();

    Map<String, String> hiveProperties = new HashMap<String, String>();
    hiveProperties.put("hive.input.format", "org.apache.hadoop.hive.ql.io.HiveInputFormat");
    // 128Mb = 134217728B
    hiveProperties.put("mapred.min.split.size", "134217728");

    Configuration myHiveConfig =
        new Configuration().withClassification("hive-site").withProperties(hiveProperties);

    configurations.add(myHiveConfig);

    return configurations.toArray(new Configuration[configurations.size()]);
  }

  /**
   * Selects the applications needed in the launch. In this case they are chosen by name.
   *
   * @return An array with the specified applications.
   */
  public Application[] getApplications() {
    Application[] applications = {
      new Application().withName("hive"),
      new Application().withName("hadoop")
    };
    return applications;
  }

  /**
   * Creates a StepConfig according to the parameters passed.
   *
   * @param name The name of the step. It is recommended that all the steps are sortable by name.
   * @param scriptPath The URL in S3 of the script.
   * @param args The arguments to pass to the script, if any.
   * @return A StepConfig that terminates the cluster on failure.
   */
  public StepConfig getHiveStep(String name, String scriptPath, String[] args) {
    return new StepConfig()
        .withName(name)
        .withActionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
        .withHadoopJarStep(new StepFactory().newRunHiveScriptStep(scriptPath, args));
  }

  /**
   * Creates the configuration of the JobFlow, depending on the number and type of instances.
   *
   * @param instanceCount The number of instances, must be at least 1.
   * @param masterInstanceType The type of instance for the master. There is at least 1 master.
   * @param slaveInstanceType The type of instance for the slave. There is instanceCount - 1 slaves.
   * @return A valid configuration for a JobFlow that finishes when it runs out of steps or fails.
   */
  public JobFlowInstancesConfig getJobFlowInstancesConfig(
      /*String ec2KeyName,*/
      int instanceCount,
      String masterInstanceType,
      String slaveInstanceType) {
    return new JobFlowInstancesConfig()
        //.withEc2KeyName(ec2KeyName)
        .withInstanceCount(instanceCount)
        .withKeepJobFlowAliveWhenNoSteps(false)
        .withMasterInstanceType(masterInstanceType)
        .withSlaveInstanceType(slaveInstanceType);
  }

  /**
   * Creates a request to run a JobFlow. It contains all the applications, configurations and steps
   * needed. The roles and release label for EC2 and EMR are determined here.
   *
   * @param applications An array of applications needed in the JobFlow.
   * @param configurations An array of configurations needed in the JobFlow.
   * @param steps The list of steps to execute in the JobFlow.
   * @param jobFlowInstancesConfig The configuration for the instances that run in this JobFlow.
   * @param logsPath The path in S3 where to store the logs.
   * @return A RunJobFlowRequest from the parameters specified.
   */
  public RunJobFlowRequest getRunJobFlowRequest(
      Application[] applications,
      Configuration[] configurations,
      List<StepConfig> steps,
      JobFlowInstancesConfig jobFlowInstancesConfig,
      String logsPath) {
    return new RunJobFlowRequest()
        .withName("EMR-4.2.0")
        .withReleaseLabel("emr-4.2.0")
        .withApplications(applications)
        .withConfigurations(configurations)
        .withSteps(steps)
        .withLogUri(logsPath)
        .withServiceRole("EMR_DefaultRole")
        .withJobFlowRole("EMR_EC2_DefaultRole")
        .withInstances(jobFlowInstancesConfig);
  }

  /**
   * Calls runJobFlow in its private instance of ElasticMapReduceClient.
   *
   * @param request The request to run.
   * @return A RunJobFlowResult according to a specified request.
   */
  public RunJobFlowResult runJobFlow(RunJobFlowRequest request) {
    return emr.runJobFlow(request);
  }

  /**
   * A method that helps the user to select an instance type among several options. Some of the
   * types are not allowed for some applications. Please check the documentation of AWS to get an
   * updated list of the instance types for the desired application like hadoop.
   *
   * @return A string representing the instance type selected.
   */
  public String selectInstanceType() {
    InstanceType[] enumTypes = InstanceType.values();
    String[] types = new String[enumTypes.length];
    for (int i = 0; i < enumTypes.length; i++) {
      types[i] = enumTypes[i].toString();
    }
    Arrays.sort(types);
    IOHelper.println("Instance type options:");
    for (int i = 0; i < types.length; i++) {
      IOHelper.print("\t" + (i + 1) + ". " + types[i]);
      if (i % 3 == 2) {
        IOHelper.println();
      }
    }

    IOHelper.println();
    IOHelper.println("Insert number option:");
    int opt = IOHelper.getInteger();
    if (opt < 1 || opt > types.length) {
      throw new IllegalArgumentException("Incorrect option.");
    }

    return types[opt - 1];
  }
}
