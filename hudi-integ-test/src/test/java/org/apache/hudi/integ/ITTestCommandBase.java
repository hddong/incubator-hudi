/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.integ;

import java.util.Arrays;

public class ITTestCommandBase extends ITTestBase{

  private static String HOODIE_GENERATE_TEST_COMMAND_APP = HOODIE_WS_ROOT + "/docker/demo/generate-test-command.sh";

  /**
   * Generate command file in docker
   *
   * @param cmd cmd write to file
   * @param filePath path of file
   * @param writeType APPEND or OVERWRITE
   */
  public void writeTestCommandToDockerTmpFile(String cmd, String filePath, String writeType)
      throws Exception {
    assert Arrays.asList("APPEND", "OVERWRITE").contains(writeType);
    String operateType = ">";
    if ("APPEND".equals(writeType)) {
      operateType = ">>";
    }
    String[] dockerCmd = new String[]{"sh", HOODIE_GENERATE_TEST_COMMAND_APP, cmd, filePath, operateType};
    executeCommandStringInDocker(ADHOC_1_CONTAINER, dockerCmd, true);
  }

  public void generateCommitInstance(String tableType, String commitType, String hoodieTableName)
      throws Exception {
    String hdfsPath = getHDFSPath(hoodieTableName);
    // Run Hoodie Java App
    String cmd = String.format("%s --table-path %s --table-type %s" +
            " --commit-type %s  --table-name %s", HOODIE_GENERATE_APP, hdfsPath,
        tableType, commitType, hoodieTableName);

    executeCommandStringInDocker(ADHOC_1_CONTAINER, cmd, true);
  }

  protected String getHDFSPath(String hoodieTableName) {
    return "hdfs://namenode/" + hoodieTableName;
  }
}
