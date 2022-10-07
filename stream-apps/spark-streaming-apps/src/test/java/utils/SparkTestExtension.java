package utils;

import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import utils.fields.SparkSessionField;

public class SparkTestExtension extends AbstractTestExtension
    implements BeforeEachCallback, AfterEachCallback {

  private static SparkSession sparkSession;

  private static boolean hasInitSpark = false;

  private static synchronized void setSparkSession(SparkSession sparkSession) {
    SparkTestExtension.sparkSession = sparkSession;
  }

  private static synchronized void setHasInitSpark(boolean hasInitSpark) {
    SparkTestExtension.hasInitSpark = hasInitSpark;
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    if (!hasInitSpark) {
      initSpark();
    }
    setHasInitSpark(true);

    patchField(extensionContext, SparkSessionField.class, sparkSession);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    if (hasInitSpark) {
      stopSpark();
    }
    setHasInitSpark(false);
  }

  private void initSpark() {
    SparkConf sparkConf =
        (new SparkConf())
            .setMaster("local[1]")
            .setAppName("test")
            .set("spark.ui.enabled", "false")
            .set("spark.app.id", UUID.randomUUID().toString())
            .set("spark.driver.host", "localhost")
            .set("spark.sql.shuffle.partitions", "1")
            // S3 conf
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .set("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
            .set("spark.hadoop.fs.s3a.path.style.access", "true")
            .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:19000")
            .set(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .set("spark.hadoop.fs.s3a.access.key", "minio_root")
            .set("spark.hadoop.fs.s3a.secret.key", "minio_pass");

    setSparkSession(SparkSession.builder().config(sparkConf).getOrCreate());
  }

  private void stopSpark() {
    sparkSession.stop();
  }
}
