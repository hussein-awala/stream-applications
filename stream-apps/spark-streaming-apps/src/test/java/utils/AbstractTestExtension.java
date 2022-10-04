package utils;

import java.lang.reflect.Field;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

public abstract class AbstractTestExtension {
  void patchField(
      ExtensionContext extensionContext,
      Class<? extends java.lang.annotation.Annotation> annotationType,
      Object value)
      throws IllegalAccessException {
    Object testInstance = extensionContext.getRequiredTestInstance();
    List<Field> fieldsToInject =
        AnnotationSupport.findAnnotatedFields(
            extensionContext.getRequiredTestClass(), annotationType);
    for (Field field : fieldsToInject) {
      field.set(testInstance, value);
    }
  }
}
