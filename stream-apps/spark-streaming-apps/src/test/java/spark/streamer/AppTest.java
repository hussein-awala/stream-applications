/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package spark.streamer;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import spark.stream.runners.KafkaToConsole;

public class AppTest {
  @Test
  public void testAppHasAGreeting() {
    KafkaToConsole classUnderTest = new KafkaToConsole();
    assertNotNull("app should have a greeting", classUnderTest.toString());
  }
}
