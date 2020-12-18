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

package org.apache.hudi.integ.command;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.integ.ITTestCommandBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test class for CompactionCommand in hudi-cli module.
 */
public class ITTestCompactionCommand extends ITTestCommandBase {

  private static final String HUDI_CLI_TOOL = HOODIE_WS_ROOT + "/hudi-cli/hudi-cli.sh";
  private static final String SCHEDULE_COMPACT_CMD = "compaction schedule --sparkMaster local";
  private static final String SCHEDULE_COMPACT_CMD_FILE = "schedule-compact.commands";

  @Test
  public void testScheduleCompact() throws Exception {
    String tableName = "test_compaction";
    // generate commits
    generateCommitInstance(HoodieTableType.MERGE_ON_READ.toString(), "overwrite", tableName);
    generateCommitInstance(HoodieTableType.MERGE_ON_READ.toString(), "append", tableName);

    // generate cmds
    String filePath = "/tmp/" + SCHEDULE_COMPACT_CMD_FILE;
    writeTestCommandToDockerTmpFile("connect --path " + getHDFSPath(tableName), filePath, "OVERWRITE");
    String compactCommand = "compaction schedule --hoodieConfigs hoodie.compact.inline.max.delta.commits=1";
    writeTestCommandToDockerTmpFile("connect --path " + getHDFSPath(tableName), filePath, "OVERWRITE");
    writeTestCommandToDockerTmpFile(compactCommand, filePath, "APPEND");
    TestExecStartResultCallback result =
        executeCommandStringInDocker(ADHOC_1_CONTAINER, HUDI_CLI_TOOL + " --cmdfile " + filePath, true);

    String expected = "";
    assertTrue(result.getStderr().toString().contains(expected));
  }
}
