package com.google.protobuf;

@SuppressWarnings("HideUtilityClassConstructor")
public class ProtobufInternalUtils {
  public static final String PB_OUTER_CLASS_SUFFIX = "OuterClass";

  /** convert underscore name to camel name. */
  public static String underScoreToCamelCase(String name, boolean capNext) {
    return SchemaUtil.toCamelCase(name, capNext);
  }

  public static String getFullJavaName(Descriptors.Descriptor descriptor) {
    if (null != descriptor.getContainingType()) {
      // nested type
      String parentJavaFullName = getFullJavaName(descriptor.getContainingType());
      return parentJavaFullName + "." + descriptor.getName();
    } else {
      // top level message
      String outerProtoName = getOuterProtoPrefix(descriptor.getFile());
      return outerProtoName + descriptor.getName();
    }
  }

  public static String getFullJavaName(Descriptors.EnumDescriptor enumDescriptor) {
    if (null != enumDescriptor.getContainingType()) {
      return getFullJavaName(enumDescriptor.getContainingType())
          + "."
          + enumDescriptor.getName();
    } else {
      String outerProtoName = getOuterProtoPrefix(enumDescriptor.getFile());
      return outerProtoName + enumDescriptor.getName();
    }
  }

  public static String getOuterProtoPrefix(Descriptors.FileDescriptor fileDescriptor) {
    String javaPackageName =
        fileDescriptor.getOptions().hasJavaPackage()
            ? fileDescriptor.getOptions().getJavaPackage()
            : fileDescriptor.getPackage();
    if (fileDescriptor.getOptions().getJavaMultipleFiles()) {
      return javaPackageName + ".";
    } else {
      String outerClassName = getOuterClassName(fileDescriptor);
      return javaPackageName + "." + outerClassName + ".";
    }
  }

  public static String getOuterClassName(Descriptors.FileDescriptor fileDescriptor) {
    if (fileDescriptor.getOptions().hasJavaOuterClassname()) {
      return fileDescriptor.getOptions().getJavaOuterClassname();
    } else {
      String[] fileNames = fileDescriptor.getName().split("/");
      String fileName = fileNames[fileNames.length - 1];
      String outerName = underScoreToCamelCase(fileName.split("\\.")[0], true);
      // https://developers.google.com/protocol-buffers/docs/reference/java-generated#invocation
      // The name of the wrapper class is determined by converting the base name of the .proto
      // file to camel case if the java_outer_classname option is not specified.
      // For example, foo_bar.proto produces the class name FooBar. If there is a service,
      // enum, or message (including nested types) in the file with the same name,
      // "OuterClass" will be appended to the wrapper class's name.
      boolean hasSameNameMessage =
          fileDescriptor.getMessageTypes().stream()
              .anyMatch(f -> f.getName().equals(outerName));
      boolean hasSameNameEnum =
          fileDescriptor.getEnumTypes().stream()
              .anyMatch(f -> f.getName().equals(outerName));
      boolean hasSameNameService =
          fileDescriptor.getServices().stream()
              .anyMatch(f -> f.getName().equals(outerName));
      if (hasSameNameMessage || hasSameNameEnum || hasSameNameService) {
        return outerName + PB_OUTER_CLASS_SUFFIX;
      } else {
        return outerName;
      }
    }
  }
}
