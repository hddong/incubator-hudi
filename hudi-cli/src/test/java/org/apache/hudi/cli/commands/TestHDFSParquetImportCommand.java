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

package org.apache.hudi.cli.commands;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.QuickstartUtils;
import org.apache.hudi.cli.AbstractShellIntegrationTest;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.springframework.shell.core.CommandResult;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHDFSParquetImportCommand extends AbstractShellIntegrationTest {

  @Test
  public void init() throws IOException {
    SparkSession spark = SparkSession.builder().getOrCreate();
    List<Driver> dataSet = Arrays.asList(new Driver(QuickstartUtils.DataGenerator.generateRandomString()),
        new Driver(QuickstartUtils.DataGenerator.generateRandomString()),
        new Driver(QuickstartUtils.DataGenerator.generateRandomString()));

    Dataset df = spark.createDataFrame(dataSet, Driver.class);
    String tableName = "test_table";
    String targetPath = basePath + File.separator + tableName;
    String srcPath = basePath + File.separator + "data";
    if (!new File(srcPath).exists()) {
      new File(srcPath).mkdir();
    }
    df.write().mode(SaveMode.Overwrite).parquet(srcPath);

    FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());

    Arrays.stream(fs.listStatus(new Path(srcPath))).map(FileStatus::getPath).forEach(System.out::println);

    String schemaFilePath = this.getClass().getClassLoader().getResource("schema.json").getPath();

    SparkEnvCommand.env.put("SPARK_MASTER", "local");
    SparkEnvCommand.env.put("SPARK_HOME", "~/soft/spark-2.4.5");
    String command = String.format("hdfsparquetimport --srcPath %s --targetPath %s --tableName %s --tableType %s --rowKeyField %s"
        + " --partitionPathField %s --parallelism %s --schemaFilePath %s --format %s --sparkMemory %s --retry %s",
        basePath, targetPath, tableName, "COPY_ON_WRITE", "uuid", "timestamp", "2", schemaFilePath, "parquet", "2G", "1");
    CommandResult cr = getShell().executeCommand(command);
    assertTrue(cr.isSuccess());

    // Check hudi table exist
    String metaPath = targetPath + File.separator + HoodieTableMetaClient.METAFOLDER_NAME;
    assertTrue("Hoodie table not exist.", new File(metaPath).exists());

    // Load meta data
    new TableCommand().connect(targetPath, TimelineLayoutVersion.VERSION_1, false, 2000, 300000, 7);
    metaClient = HoodieCLI.getTableMetaClient();

    assertEquals("Should only 1 commit.", 1, metaClient.getActiveTimeline().getCommitsTimeline().countInstants());

    // Read result and check equals
    Dataset<Row> result = HoodieClientTestUtils.read(jsc, targetPath, spark.sqlContext(), fs, targetPath + "/0.0/*");
    List<String> resultList =
        result.select("uuid").collectAsList().stream().map(row -> (String)row.get(0))
            .sorted().collect(Collectors.toList());

    assertEquals(resultList, dataSet.stream().map(Driver::getUuid).sorted().collect(Collectors.toList()));
  }

  public class Driver implements Serializable {
    String uuid;
    String timestamp;
    String riderName;
    String driverName;
    double beginLat;
    double beginLon;
    double endLat;
    double endLon;
    double fare;

    public Driver(String randomSuffix) {
      uuid = UUID.randomUUID().toString();
      timestamp = "0.0";
      riderName = "rider-" + randomSuffix;
      driverName = "driver-" + randomSuffix;
      Random rand = new Random(666);
      beginLat = rand.nextDouble();
      beginLon = rand.nextDouble();
      endLat = rand.nextDouble();
      endLon = rand.nextDouble();
      fare = rand.nextDouble() * 100;
    }

    public String getUuid() {
      return uuid;
    }

    public String getTimestamp() {
      return timestamp;
    }

    public String getRiderName() {
      return riderName;
    }

    public String getDriverName() {
      return driverName;
    }

    public double getBeginLat() {
      return beginLat;
    }

    public double getBeginLon() {
      return beginLon;
    }

    public double getEndLat() {
      return endLat;
    }

    public double getEndLon() {
      return endLon;
    }

    public double getFare() {
      return fare;
    }
  }
}
