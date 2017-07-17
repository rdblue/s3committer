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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;


public class S3DirectoryOutputCommitter extends S3MultipartOutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(
      S3DirectoryOutputCommitter.class);

  public S3DirectoryOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    Path outputPath = getOutputPath(context);
    // use the FS implementation because it will check for _$folder$
    FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
    if (fs.exists(outputPath)) {
      switch (getMode(context)) {
        case FAIL:
          throw new AlreadyExistsException(
              "Output path already exists: " + outputPath);
        case APPEND:
        case REPLACE:
          // do nothing.
          // removing the directory, if overwriting is done in commitJob, in
          // case there is a failure before commit.
      }
    }

    super.setupJob(context);
  }

  // TODO: handle aborting commits if delete or exists throws an exception
  @Override
  public void commitJob(JobContext context) throws IOException {
    Path outputPath = getOutputPath(context);
    // use the FS implementation because it will check for _$folder$
    FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
    if (fs.exists(outputPath)) {
      switch (getMode(context)) {
        case FAIL:
          // this was checked in setupJob, but this avoids some cases where
          // output was created while the job was processing
          throw new AlreadyExistsException(
              "Output path already exists: " + outputPath);
        case APPEND:
          // do nothing
          break;
        case REPLACE:
          LOG.info("Removing output path to be replaced: " + outputPath);
          if (!fs.delete(outputPath, true /* recursive */ )) {
            throw new IOException(
                "Failed to delete existing output directory for replace:" +
                outputPath);
          }
          break;
        default:
          throw new RuntimeException(
              "Unknown conflict resolution mode: " + getMode(context));
      }
    }

    super.commitJob(context);
  }
}
