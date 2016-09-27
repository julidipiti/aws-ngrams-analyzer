import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.model.Application;
import com.amazonaws.services.elasticmapreduce.model.Configuration;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * Main class to launch the analyzer.
 *
 * @author julidipiti
 */
public class Main {

  private static S3Helper s3h;
  private static EMRHelper emrh;
  private static final String ngramsFullPath =
      "s3://datasets.elasticmapreduce/ngrams/books/20090715/";
  private static final String genericRegex = "^\\\\\\p{Ll}+(\\\\\\-)?\\\\\\p{Ll}+$";
  private static String bucketName = "ana-" + UUID.randomUUID();
  private static String scriptsRelativePath = "EMR/HiveScripts/";
  private static String scriptsFullPath = "s3://" + bucketName + "/" + scriptsRelativePath;
  private static String OutputFullPath = "s3://" + bucketName + "/EMR/Output/";
  private static String logsPath = "s3://" + bucketName + "/EMR/Logs/";
  private static int stepCounter = 1;

  /**
   * Entry point.
   *
   * @param args Arguments to pass, if any.
   * @throws IOException An exception that indicates some problem in the execution.
   */
  public static void main(String[] args) throws IOException {

    // Credentials needed to execute on AWS.
    AWSCredentials credentials;
    Region region = Region.getRegion(Regions.US_EAST_1);

    try {
      credentials = new ProfileCredentialsProvider("default").getCredentials();
    } catch (Exception exc) {
      throw new AmazonClientException(
          "Cannot load the credentials from the credential profiles" + "file. " + exc);
    }

    s3h = new S3Helper(credentials, region);

    // Creates bucket to store the output.
    s3h.createBucket(bucketName);

    emrh = new EMRHelper(credentials, region);

    IOHelper.println();
    IOHelper.println("Select the main language to analyze:");
    String language1 = s3h.selectLanguageOption();

    IOHelper.println();
    IOHelper.println(
        "Select the language from which to extract the foreignisms, or the same as before if you"
            + " are only interested in neologisms:");

    String language2 = s3h.selectLanguageOption();

    IOHelper.println();
    IOHelper.println("Select master instance type:");
    String masterInstanceType = emrh.selectInstanceType();

    IOHelper.println();
    IOHelper.println("Select slave instance type:");
    String slaveInstanceType = emrh.selectInstanceType();

    s3h.uploadHiveScripts(bucketName, scriptsRelativePath);

    IOHelper.println();
    IOHelper.println(
        "Insert the year from which to start the analisis, between 1700 and 2008 (e.g., 1800):");
    int fromYear = IOHelper.getInteger();

    IOHelper.println();
    IOHelper.println(
        "Insert the year to end the analisis, between the previous selected number and 2008 (e.g., "
            + "1820):");
    int toYear = IOHelper.getInteger();

    IOHelper.println();
    IOHelper.println(
        "Insert the size of the window, which must be smaller than the difference of the years "
            + "(e.g., 5):");
    int windowSize = IOHelper.getInteger();

    IOHelper.println();
    IOHelper.println(
        "Insert the percent of years needed for a gram, " + "between 0.1 and 1.0 (e.g., 0.8):");
    double percentOfYears = IOHelper.getDouble();

    runFinders(
        masterInstanceType,
        slaveInstanceType,
        language1,
        language1.replace('-', '_'),
        language2,
        language2.replace('-', '_'),
        fromYear,
        toYear,
        windowSize,
        percentOfYears);
  }

  /**
   * Launches an EMR cluster on AWS that analyzes the Google Books Ngrams looking for neologisms and
   * foreignisms between 2 languages.
   *
   * @param masterInstanceType The type of instance for the only master.
   * @param slaveInstanceType The type of instance for all the slaves.
   * @param language1 The main language to analyze.
   * @param ngramsTable1 The name to give to the table of language1.
   * @param language2 The language that sources the foreignisms to language1.
   * @param ngramsTable2 The name to give to the table of language2.
   * @param fromYear The year to start from.
   * @param toYear The last year to analyze.
   * @param windowSize The size of the window to shift and analyze between fromYear and toYear.
   * @param percentOfYears The ratio of the years that a word need to be in to be considered as a
   *     part of the dictionary.
   * @throws IllegalArgumentException Accuses some problem with the input.
   */
  static void runFinders(
      String masterInstanceType,
      String slaveInstanceType,
      String language1,
      String ngramsTable1,
      String language2,
      String ngramsTable2,
      int fromYear,
      int toYear,
      int windowSize,
      double percentOfYears)
      throws IllegalArgumentException {
    if (windowSize < 1) {
      throw new IllegalArgumentException("windowSize must be at least 1.");
    }
    if (fromYear < 1700 || fromYear > 2008 || toYear < 1700 || toYear > 2008) {
      throw new IllegalArgumentException("fromYear and toYear must be between 1700 and 2008");
    }
    if (fromYear >= toYear) {
      throw new IllegalArgumentException("fromYear must be less than toYear.");
    }
    if (fromYear + windowSize > toYear) {
      throw new IllegalArgumentException(
          "There are not enough years to shift the window. Make sure "
              + "fromYear + windowSize is less than toYear.");
    }
    if (percentOfYears < 0.1 || percentOfYears > 1.0) {
      throw new IllegalArgumentException("percentOfYears must be between 0.1 and 1.0");
    }

    List<StepConfig> steps = new LinkedList<>();
    steps.addAll(
        getHiveStepsForCreatingDictionary(
            ngramsFullPath + language1 + "/1gram/",
            ngramsTable1,
            fromYear,
            toYear,
            windowSize,
            percentOfYears));

    if (!language1.equals(language2)) {
      steps.addAll(
          getHiveStepsForCreatingDictionary(
              ngramsFullPath + language2 + "/1gram/",
              ngramsTable2,
              fromYear,
              toYear,
              windowSize,
              percentOfYears));

      steps.addAll(
          getHiveStepsForForeignismsFinder(
              ngramsTable1, ngramsTable2, fromYear, toYear, windowSize, percentOfYears));
    }

    steps.addAll(
        getHiveStepsForNeologismsFinder(
            ngramsTable1, fromYear, toYear, windowSize, percentOfYears));

    Application[] applications = emrh.getApplications();
    Configuration[] configurations = emrh.getConfigurations();

    IOHelper.println();
    IOHelper.println("Insert the size of the cluster, between 1 and 20 " + "(e.g., 10):");
    int clusterSize = IOHelper.getInteger();

    // Uncomment these lines and pass the parameter to the getRunJobFlowRequest if you want to use
    // an ec2-key on the connection.
    //System.out.println();
    //System.out.println("Insert the name of the ec2-key (e.g., my-key):");
    //String ec2key = IOHelper.getWord();

    JobFlowInstancesConfig jobFlowInstancesConfig =
        emrh.getJobFlowInstancesConfig(
            /* ec2key ,*/ clusterSize, masterInstanceType, slaveInstanceType);

    RunJobFlowRequest request =
        emrh.getRunJobFlowRequest(
            applications, configurations, steps, jobFlowInstancesConfig, logsPath);

    RunJobFlowResult result = emrh.runJobFlow(request);
    IOHelper.println();
    IOHelper.println("Launching job with id:");
    IOHelper.println(result.getJobFlowId());
    IOHelper.println();
  }

  /**
   * Gets the steps needed for finding foreignisms between language1 and language2 and exporting
   * them to S3.
   *
   * @param ngramsLocation1 The location on S3 of the main ngrams.
   * @param ngramsLocation2 The location on S3 of the other ngrams.
   * @param ngramsTable1 The name of the table for the main ngrams.
   * @param ngramsTable2 The name of the table for the other ngrams.
   * @param fromYear The year to start from.
   * @param toYear The last year to analyze.
   * @param windowSize Size of the window.
   * @param percentOfYears Percent of years needed for a ngram to be in the window.
   * @return A list of the steps needed for launching the foreignisms finder.
   */
  static List<StepConfig> getHiveStepsForForeignismsFinder(
      String ngramsTable1,
      String ngramsTable2,
      int fromYear,
      int toYear,
      int windowSize,
      double percentOfYears) {
    List<StepConfig> steps = new LinkedList<StepConfig>();

    steps.add(
        emrh.getHiveStep(
            getStepName(),
            scriptsFullPath + "ExportForeignisms.q",
            createParameters(
                "ngramsTable1=" + ngramsTable1,
                "ngramsTable2=" + ngramsTable2,
                "output=" + OutputFullPath + ngramsTable1 + "/Foreignisms/" + ngramsTable2)));

    return steps;
  }

  /**
   * Gets the steps needed for creating a dictionary made out of all the words in a language, and
   * exporting them to S3.
   *
   * @param ngramsLocation The location on S3 of the ngrams.
   * @param ngramsTable The name of the table for the ngrams.
   * @param fromYear The year to start from.
   * @param toYear The last year to analyze.
   * @param windowSize Size of the window.
   * @param percentOfYears Percent of years needed for a ngram to be in the window.
   * @return A list of steps needed for creating a dictionary.
   */
  static List<StepConfig> getHiveStepsForCreatingDictionary(
      String ngramsLocation,
      String ngramsTable,
      int fromYear,
      int toYear,
      int windowSize,
      double percentOfYears) {

    List<StepConfig> steps = new LinkedList<StepConfig>();

    steps.add(
        emrh.getHiveStep(
            getStepName(),
            scriptsFullPath + "ImportNgrams.q",
            createParameters(
                "ngramsLocation=" + ngramsLocation,
                "regex=" + genericRegex,
                "ngramsTable=" + ngramsTable)));

    steps.add(
        emrh.getHiveStep(
            getStepName(),
            scriptsFullPath + "CreateWindow.q",
            createParameters(
                "ngramsTable=" + ngramsTable,
                "fromYear=" + fromYear,
                "toYear=" + (fromYear + windowSize))));

    for (int i = fromYear + windowSize; i <= toYear; i++) {
      steps.add(
          emrh.getHiveStep(
              getStepName(),
              scriptsFullPath + "ShiftWindow.q",
              createParameters(
                  "ngramsTable=" + ngramsTable, "newYear=" + i, "windowSize=" + windowSize)));
    }

    steps.add(
        emrh.getHiveStep(
            getStepName(),
            scriptsFullPath + "ExportDictionary.q",
            createParameters(
                "ngramsTable=" + ngramsTable,
                "windowSize=" + windowSize,
                "percentOfYears=" + percentOfYears,
                "output=" + OutputFullPath + ngramsTable + "/Dic")));

    return steps;
  }

  /**
   * Gets the steps needed for finding neologisms in a language and exporting them to S3.
   *
   * @param ngramsTable The name of the table for the ngrams.
   * @param fromYear The year to start from.
   * @param toYear The last year to analyze.
   * @param windowSize Size of the window.
   * @param percentOfYears Percent of years needed for a ngram to be in the window.
   * @return A list of steps needed for launching the neologisms finder.
   */
  static List<StepConfig> getHiveStepsForNeologismsFinder(
      String ngramsTable, int fromYear, int toYear, int windowSize, double percentOfYears) {
    List<StepConfig> steps = new LinkedList<StepConfig>();

    for (int i = fromYear + windowSize; i <= toYear; i++) {
      steps.add(
          emrh.getHiveStep(
              getStepName(),
              scriptsFullPath + "ProcessNeologisms.q",
              createParameters("ngramsTable=" + ngramsTable, "year=" + i)));
    }

    steps.add(
        emrh.getHiveStep(
            getStepName(),
            scriptsFullPath + "ExportNeologisms.q",
            createParameters(
                "ngramsTable=" + ngramsTable, "output=" + OutputFullPath + ngramsTable + "/Neo")));

    return steps;
  }

  /**
   * Creates an array of parameters for hive scripts, which need the "-d" option before every
   * parameter.
   *
   * @param parameters A few strings to pass as parameters.
   * @return A well-formatted string for passing paramters.
   */
  private static String[] createParameters(String... parameters) {
    String[] parametersArray = new String[parameters.length * 2];
    for (int i = 0; i < parameters.length; i++) {
      parametersArray[i * 2] = "-d";
      parametersArray[i * 2 + 1] = parameters[i];
    }
    return parametersArray;
  }

  /**
   * Gets a generic name for EMR steps. There could be up to 256 steps in the queue of steps, so the
   * names have a 3-digit number id.
   * @return The name of the next step.
   */
  private static String getStepName() {
    return "Step-" + String.format("%03d", stepCounter++);
  }
}
