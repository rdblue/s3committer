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

import com.amazonaws.services.s3.AmazonS3;
import com.netflix.bdp.s3.TestUtil.ClientErrors;
import com.netflix.bdp.s3.TestUtil.ClientResults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Committer subclass that uses a mocked AmazonS3Client for testing.
 */
class MockedS3Committer extends S3MultipartOutputCommitter {

  public final ClientResults results = new ClientResults();
  public final ClientErrors errors = new ClientErrors();
  private final AmazonS3 mockClient = TestUtil.newMockClient(results, errors);

  public MockedS3Committer(Path outputPath, JobContext context)
      throws IOException {
    super(outputPath, context);
  }

  public MockedS3Committer(Path outputPath, TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
  }

  @Override
  protected AmazonS3 findClient(Path path, Configuration conf) {
    return mockClient;
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    super.commitJob(context);
    Configuration conf = context.getConfiguration();
    try {
      String jobCommitterPath = conf.get("mock-results-file");
      if (jobCommitterPath != null) {
        try (ObjectOutputStream out = new ObjectOutputStream(
            FileSystem.getLocal(conf).create(new Path(jobCommitterPath), false))) {
          out.writeObject(results);
        }
      }
    } catch (Exception e) {
      // do nothing, the test will fail
    }
  }

}
