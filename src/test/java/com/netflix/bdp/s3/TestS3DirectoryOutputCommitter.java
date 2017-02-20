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
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.Callable;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestS3DirectoryOutputCommitter extends TestUtil.JobCommitterTest<S3DirectoryOutputCommitter> {
  @Override
  S3DirectoryOutputCommitter newJobCommitter() throws Exception {
    return new S3DirectoryOutputCommitter(OUTPUT_PATH, getJob());
  }

  @Test
  public void testDefaultConflictResolution() throws Exception {
    FileSystem mockS3 = getMockS3();

    when(mockS3.exists(OUTPUT_PATH)).thenReturn(true);

    final S3DirectoryOutputCommitter committer = newJobCommitter();

    TestUtil.assertThrows("Should throw an exception because the path exists",
        AlreadyExistsException.class, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            committer.setupJob(getJob());
            return null;
          }
        });

    TestUtil.assertThrows("Should throw an exception because the path exists",
        AlreadyExistsException.class, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            committer.commitJob(getJob());
            return null;
          }
        });

    reset(mockS3);
    when(mockS3.exists(OUTPUT_PATH)).thenReturn(false);

    committer.setupJob(getJob());
    verify(mockS3).exists(OUTPUT_PATH);
    verifyNoMoreInteractions(mockS3);

    reset(mockS3);
    when(mockS3.exists(OUTPUT_PATH)).thenReturn(false);

    committer.commitJob(getJob());
    verify(mockS3).exists(OUTPUT_PATH);
    verifyNoMoreInteractions(mockS3);
  }

  @Test
  public void testFailConflictResolution() throws Exception {
    FileSystem mockS3 = getMockS3();

    when(mockS3.exists(OUTPUT_PATH)).thenReturn(true);

    getJob().getConfiguration().set(S3Committer.CONFLICT_MODE, "fail");

    final S3DirectoryOutputCommitter committer = newJobCommitter();

    TestUtil.assertThrows("Should throw an exception because the path exists",
        AlreadyExistsException.class, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            committer.setupJob(getJob());
            return null;
          }
        });

    TestUtil.assertThrows("Should throw an exception because the path exists",
        AlreadyExistsException.class, new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            committer.commitJob(getJob());
            return null;
          }
        });

    reset(mockS3);
    when(mockS3.exists(OUTPUT_PATH)).thenReturn(false);

    committer.setupJob(getJob());
    verify(mockS3).exists(OUTPUT_PATH);
    verifyNoMoreInteractions(mockS3);

    reset(mockS3);
    when(mockS3.exists(OUTPUT_PATH)).thenReturn(false);

    committer.commitJob(getJob());
    verify(mockS3).exists(OUTPUT_PATH);
    verifyNoMoreInteractions(mockS3);
  }

  @Test
  public void testAppendConflictResolution() throws Exception {
    FileSystem mockS3 = getMockS3();

    when(mockS3.exists(OUTPUT_PATH)).thenReturn(true);

    getJob().getConfiguration().set(S3Committer.CONFLICT_MODE, "append");

    final S3DirectoryOutputCommitter committer = newJobCommitter();

    committer.setupJob(getJob());
    verify(mockS3).exists(OUTPUT_PATH);

    Mockito.reset(mockS3);
    when(mockS3.exists(OUTPUT_PATH)).thenReturn(true);

    committer.commitJob(getJob());
    verify(mockS3).exists(OUTPUT_PATH);
    verifyNoMoreInteractions(mockS3);
  }

  @Test
  public void testReplaceConflictResolution() throws Exception {
    FileSystem mockS3 = getMockS3();

    when(mockS3.exists(OUTPUT_PATH)).thenReturn(true);

    getJob().getConfiguration().set(S3Committer.CONFLICT_MODE, "replace");

    final S3DirectoryOutputCommitter committer = newJobCommitter();

    committer.setupJob(getJob());
    verify(mockS3).exists(OUTPUT_PATH);
    verifyNoMoreInteractions(mockS3);

    Mockito.reset(mockS3);
    when(mockS3.exists(OUTPUT_PATH)).thenReturn(true);
    when(mockS3.delete(OUTPUT_PATH, true)).thenReturn(true);

    committer.commitJob(getJob());
    verify(mockS3).exists(OUTPUT_PATH);
    verify(mockS3).delete(OUTPUT_PATH, true);
    verifyNoMoreInteractions(mockS3);
  }
}
