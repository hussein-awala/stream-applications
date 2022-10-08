package spark.utils;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Testcontainers;
import utils.MinioTestExtension;
import utils.SparkTestExtension;
import utils.fields.SparkSessionField;

@ExtendWith(SparkTestExtension.class)
@ExtendWith(MinioTestExtension.class)
@Testcontainers
public class HiveDatabasesCreatorTest {
  @SparkSessionField public SparkSession sparkSession;

  @Test
  void Given_SparkExtension_When_CallSparkSession_Then_ShouldNotBeNull() {
    Assertions.assertNotNull(sparkSession);
  }

  @Test
  void GivenHiveDatabaseNameArgs_thenTheNewDbShouldBeCreated() {
    Row[] dbs = (Row[]) sparkSession.sql("SHOW DATABASES;").collect();
    Assertions.assertEquals(1, dbs.length);
    Assertions.assertEquals("default", dbs[0].getString(0));

    HiveDatabasesCreator hiveDatabasesCreator = new HiveDatabasesCreator();
    hiveDatabasesCreator.createDatabase("test_db", sparkSession);

    dbs = (Row[]) sparkSession.sql("SHOW DATABASES;").collect();
    Assertions.assertEquals(2, dbs.length);
    Assertions.assertEquals("test_db", dbs[1].getString(0));
  }
}
