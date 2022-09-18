package conf;

import org.apache.spark.SparkConf;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.util.ArrayList;

public class SparkConfBuilder {
    private String appName;
    private String master;
    private final ArrayList<Tuple2<String, String>> conf;

    public SparkConfBuilder() {
        this.conf = new ArrayList<>();
    }

    public SparkConfBuilder(String appName, String master) {
        this();
        this.appName = appName;
        this.master = master;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public SparkConfBuilder addS3Conf() {
        this.conf.add(new Tuple2<>("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"));
        this.conf.add(new Tuple2<>("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A"));
        this.conf.add(new Tuple2<>("spark.hadoop.fs.s3a.path.style.access", "true"));
        this.conf.add(new Tuple2<>("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000"));
        this.conf.add(new Tuple2<>("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"));
        this.conf.add(new Tuple2<>("spark.hadoop.fs.s3a.access.key", "minio_root"));
        this.conf.add(new Tuple2<>("spark.hadoop.fs.s3a.secret.key", "minio_pass"));
        return this;
    }

    public SparkConfBuilder addHudiConf() {
        this.conf.add(new Tuple2<>("spark.serializer", "org.apache.spark.serializer.KryoSerializer"));
        this.conf.add(new Tuple2<>("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog"));
        this.conf.add(new Tuple2<>("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"));
        return this;
    }

    public SparkConf build() {
        SparkConf sparkConf = new SparkConf();
        if (this.appName != null) sparkConf.setAppName(this.appName);
        if (this.master != null) sparkConf.setMaster(this.master);
        sparkConf.setAll(JavaConverters.collectionAsScalaIterableConverter(this.conf).asScala().toSeq());
        return sparkConf;
    }

}
