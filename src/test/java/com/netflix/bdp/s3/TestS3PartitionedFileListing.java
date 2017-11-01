/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.bdp.s3;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.netflix.bdp.s3.util.Paths.getRelativePath;

public class TestS3PartitionedFileListing extends TestUtil.TaskCommitterTest<S3PartitionedOutputCommitter> {
  @Override
  S3PartitionedOutputCommitter newJobCommitter() throws IOException {
    return new S3PartitionedOutputCommitter(OUTPUT_PATH, getJob());
  }

  @Override
  S3PartitionedOutputCommitter newTaskCommitter() throws IOException {
    return new S3PartitionedOutputCommitter(OUTPUT_PATH, getTAC());
  }

  @Test
  public void testTaskOutputListing() throws Exception {
    S3PartitionedOutputCommitter committer = newTaskCommitter();

    // create files in the attempt path that should be found by getTaskOutput
    Path attemptPath = committer.getTaskAttemptPath(getTAC());
    FileSystem attemptFS = attemptPath.getFileSystem(getTAC().getConfiguration());
    attemptFS.delete(attemptPath, true);

    Set<String> expectedFiles = Sets.newHashSet();
    for (String dateint : Arrays.asList("20161115", "20161116")) {
      for (String hour : Arrays.asList("13", "14")) {
        String relative = "dateint=" + dateint + "/hour=" + hour +
            "/" + UUID.randomUUID().toString() + ".parquet";
        expectedFiles.add(relative);
        attemptFS.create(new Path(attemptPath, relative)).close();
      }
    }

    List<FileStatus> attemptFiles = committer.getTaskOutput(getTAC());
    Set<String> actualFiles = Sets.newHashSet();
    for (FileStatus stat : attemptFiles) {
      String relative = getRelativePath(attemptPath, stat.getPath());
      actualFiles.add(relative);
    }

    Assert.assertEquals("File sets should match", expectedFiles, actualFiles);

    attemptFS.delete(attemptPath, true);
  }

  @Test
  public void testTaskOutputListingWithHiddenFiles() throws Exception {
    S3PartitionedOutputCommitter committer = newTaskCommitter();

    // create files in the attempt path that should be found by getTaskOutput
    Path attemptPath = committer.getTaskAttemptPath(getTAC());
    FileSystem attemptFS = attemptPath.getFileSystem(getTAC().getConfiguration());
    attemptFS.delete(attemptPath, true);

    Set<String> expectedFiles = Sets.newHashSet();
    for (String dateint : Arrays.asList("20161115", "20161116")) {
      String metadata = "dateint=" + dateint + "/" + "_metadata";
      attemptFS.create(new Path(attemptPath, metadata)).close();

      for (String hour : Arrays.asList("13", "14")) {
        String relative = "dateint=" + dateint + "/hour=" + hour +
            "/" + UUID.randomUUID().toString() + ".parquet";
        expectedFiles.add(relative);
        attemptFS.create(new Path(attemptPath, relative)).close();

        String partial = "dateint=" + dateint + "/hour=" + hour +
            "/." + UUID.randomUUID().toString() + ".partial";
        attemptFS.create(new Path(attemptPath, partial)).close();
      }
    }

    List<FileStatus> attemptFiles = committer.getTaskOutput(getTAC());
    Set<String> actualFiles = Sets.newHashSet();
    for (FileStatus stat : attemptFiles) {
      String relative = getRelativePath(attemptPath, stat.getPath());
      actualFiles.add(relative);
    }

    Assert.assertEquals("File sets should match", expectedFiles, actualFiles);

    attemptFS.delete(attemptPath, true);
  }
}
