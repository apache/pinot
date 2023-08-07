/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spotbugs.plugin;

import edu.umd.cs.findbugs.BugInstance;
import edu.umd.cs.findbugs.BugReporter;
import edu.umd.cs.findbugs.bcel.OpcodeStackDetector;
import java.util.Arrays;
import java.util.List;
import org.apache.bcel.Const;
import org.apache.bcel.classfile.Utility;


public class ShadedTypesUsedOnSpiDetector extends OpcodeStackDetector {
  private final BugReporter _bugReporter;
  private final List<String> _shadedPackages;
  private final List<String> _spiPackages;


  public static final String BUG_TYPE = "SHADED_TYPES_IN_SPI";

  public ShadedTypesUsedOnSpiDetector(BugReporter bugReporter) {
    _bugReporter = bugReporter;
    _shadedPackages = Arrays.asList("com.fasterxml.jackson", "com.google.common");
    _spiPackages = Arrays.asList("org.apache.pinot.segment.spi", "org.apache.pinot.spi");
  }

  @Override
  public void sawOpcode(int seen) {
    if (seen != Const.INVOKESTATIC && seen != Const.INVOKEVIRTUAL && seen != Const.INVOKESPECIAL) {
      return;
    }

    if (!isSpiClass(getDottedClassConstantOperand())) {
      return;
    }

    String sigConstantOperand = getSigConstantOperand();
    String returnClass = Utility.methodSignatureReturnType(sigConstantOperand, false);
    String[] argClasses = Utility.methodSignatureArgumentTypes(sigConstantOperand, false);
    if (isShadedType(returnClass)) {
      _bugReporter.reportBug(
          new BugInstance(this, BUG_TYPE, NORMAL_PRIORITY)
              .addClassAndMethod(this)
              .addSourceLine(this, getPC())
      );
    }
    for (String argClass : argClasses) {
      if (isShadedType(argClass)) {
        _bugReporter.reportBug(
            new BugInstance(this, BUG_TYPE, NORMAL_PRIORITY)
                .addClassAndMethod(this)
                .addSourceLine(this, getPC()));
      }
    }

    if (getClassConstantOperand().equals("java/lang/System") && getNameConstantOperand().equals("out")) {
      // report bug when System.out is used in code
      BugInstance bug = new BugInstance(this, "MY_BUG", NORMAL_PRIORITY)
          .addClassAndMethod(this)
          .addSourceLine(this, getPC());
      _bugReporter.reportBug(bug);
    }
  }

  private boolean isShadedType(String className) {
    return _shadedPackages.stream().anyMatch(className::startsWith);
  }

  private boolean isSpiClass(String className) {
    return _spiPackages.stream().anyMatch(className::startsWith);
  }
}
