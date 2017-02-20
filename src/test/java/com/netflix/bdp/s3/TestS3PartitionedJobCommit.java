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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestS3PartitionedJobCommit extends TestUtil.JobCommitterTest<S3PartitionedOutputCommitter> {
  @Override
  S3PartitionedOutputCommitter newJobCommitter() throws IOException {
    return new TestPartitionedOutputCommitter(getJob(), mock(AmazonS3.class));
  }

  private static class TestPartitionedOutputCommitter extends S3PartitionedOutputCommitter {
    private final AmazonS3 client;
    private TestPartitionedOutputCommitter(JobContext context, AmazonS3 client)
        throws IOException {
      super(OUTPUT_PATH, context);
      this.client = client;
    }

    @Override
    protected Object findClient(Path path, Configuration conf) {
      return client;
    }

    @Override
    protected List<S3Util.PendingUpload> getPendingUploads(JobContext context)
        throws IOException {
      List<S3Util.PendingUpload> pending = Lists.newArrayList();

      for (String dateint : Arrays.asList("20161115", "20161116")) {
        for (String hour : Arrays.asList("13", "14")) {
          String key = OUTPUT_PREFIX + "/dateint=" + dateint + "/hour=" + hour +
              "/" + UUID.randomUUID().toString() + ".parquet";
          pending.add(new S3Util.PendingUpload(null,
              MockS3FileSystem.BUCKET, key, UUID.randomUUID().toString(),
              Maps.<Integer, String>newHashMap()));
        }
      }

      return pending;
    }

    private boolean aborted = false;

    @Override
    protected void abortJobInternal(JobContext context,
                                    List<S3Util.PendingUpload> pending,
                                    boolean suppressExceptions) throws IOException {
      this.aborted = true;
      super.abortJobInternal(context, pending, suppressExceptions);
    }
  }

  @Test
  public void testDefaultFailAndAppend() throws Exception {
    FileSystem mockS3 = getMockS3();

    // both fail and append don't check. fail is enforced at the task level.
    for (String mode : Arrays.asList(null, "fail", "append")) {
      if (mode != null) {
        getJob().getConfiguration().set(S3Committer.CONFLICT_MODE, mode);
      }

      S3PartitionedOutputCommitter committer = newJobCommitter();

      // no directories exist
      committer.commitJob(getJob());
      verifyNoMoreInteractions(mockS3);

      // parent and peer directories exist
      reset(mockS3);
      when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161116")))
          .thenReturn(true);
      when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=10")))
          .thenReturn(true);
      committer.commitJob(getJob());
      verifyNoMoreInteractions(mockS3);

      // a leaf directory exists.
      // NOTE: this is not checked during job commit, the commit succeeds.
      reset(mockS3);
      when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=14")))
          .thenReturn(true);
      committer.commitJob(getJob());
      verifyNoMoreInteractions(mockS3);
    }
  }

  @Test
  public void testReplace() throws Exception {
    FileSystem mockS3 = getMockS3();

    getJob().getConfiguration().set(S3Committer.CONFLICT_MODE, "replace");

    S3PartitionedOutputCommitter committer = newJobCommitter();

    committer.commitJob(getJob());
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=13"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=14"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=13"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=14"));
    verifyNoMoreInteractions(mockS3);

    // parent and peer directories exist
    reset(mockS3);
    when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161115")))
        .thenReturn(true);
    when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=12")))
        .thenReturn(true);

    committer.commitJob(getJob());
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=13"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=14"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=13"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=14"));
    verifyNoMoreInteractions(mockS3);

    // partition directories exist and should be removed
    reset(mockS3);
    when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=12")))
        .thenReturn(true);
    when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=13")))
        .thenReturn(true);
    when(mockS3
        .delete(
            new Path(OUTPUT_PATH, "dateint=20161115/hour=13"),
            true /* recursive */ ))
        .thenReturn(true);

    committer.commitJob(getJob());
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=13"));
    verify(mockS3).delete(
        new Path(OUTPUT_PATH, "dateint=20161115/hour=13"),
        true /* recursive */ );
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=14"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=13"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=14"));
    verifyNoMoreInteractions(mockS3);

    // partition directories exist and should be removed
    reset(mockS3);
    when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=13")))
        .thenReturn(true);
    when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=14")))
        .thenReturn(true);
    when(mockS3
        .delete(
            new Path(OUTPUT_PATH, "dateint=20161116/hour=13"),
            true /* recursive */ ))
        .thenReturn(true);
    when(mockS3
        .delete(
            new Path(OUTPUT_PATH, "dateint=20161116/hour=14"),
            true /* recursive */ ))
        .thenReturn(true);

    committer.commitJob(getJob());
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=13"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=14"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=13"));
    verify(mockS3).delete(
        new Path(OUTPUT_PATH, "dateint=20161116/hour=13"),
        true /* recursive */ );
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=14"));
    verify(mockS3).delete(
        new Path(OUTPUT_PATH, "dateint=20161116/hour=14"),
        true /* recursive */ );
    verifyNoMoreInteractions(mockS3);
  }

  @Test
  public void testReplaceWithExistsFailure() throws Exception {
    FileSystem mockS3 = getMockS3();

    getJob().getConfiguration().set(S3Committer.CONFLICT_MODE, "replace");

    final S3PartitionedOutputCommitter committer = newJobCommitter();

    when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=13")))
        .thenReturn(true);
    when(mockS3
        .delete(
            new Path(OUTPUT_PATH, "dateint=20161115/hour=13"),
            true /* recursive */ ))
        .thenReturn(true);
    when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=14")))
        .thenThrow(new IOException("Fake IOException for exists"));

    TestUtil.assertThrows("Should throw the fake IOException",
        IOException.class, new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        committer.commitJob(getJob());
        return null;
      }
    });

    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=13"));
    verify(mockS3).delete(
        new Path(OUTPUT_PATH, "dateint=20161115/hour=13"),
        true /* recursive */ );
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=14"));
    Assert.assertTrue("Should have aborted",
        ((TestPartitionedOutputCommitter) committer).aborted);
    verifyNoMoreInteractions(mockS3);
  }

  @Test
  public void testReplaceWithDeleteFailure() throws Exception {
    FileSystem mockS3 = getMockS3();

    getJob().getConfiguration().set(S3Committer.CONFLICT_MODE, "replace");

    final S3PartitionedOutputCommitter committer = newJobCommitter();

    when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=14")))
        .thenReturn(true);
    when(mockS3
        .delete(
            new Path(OUTPUT_PATH, "dateint=20161116/hour=14"),
            true /* recursive */ ))
        .thenThrow(new IOException("Fake IOException for delete"));

    TestUtil.assertThrows("Should throw the fake IOException",
        IOException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            committer.commitJob(getJob());
            return null;
          }
        });

    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=13"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=14"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=13"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=14"));
    verify(mockS3).delete(
        new Path(OUTPUT_PATH, "dateint=20161116/hour=14"),
        true /* recursive */ );
    Assert.assertTrue("Should have aborted",
        ((TestPartitionedOutputCommitter) committer).aborted);
    verifyNoMoreInteractions(mockS3);
  }

  @Test
  public void testReplaceWithDeleteFalse() throws Exception {
    FileSystem mockS3 = getMockS3();

    getJob().getConfiguration().set(S3Committer.CONFLICT_MODE, "replace");

    final S3PartitionedOutputCommitter committer = newJobCommitter();

    when(mockS3.exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=13")))
        .thenReturn(true);
    when(mockS3
        .delete(
            new Path(OUTPUT_PATH, "dateint=20161116/hour=13"),
            true /* recursive */ ))
        .thenReturn(false);

    TestUtil.assertThrows("Should throw an IOException",
        IOException.class, new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            committer.commitJob(getJob());
            return null;
          }
        });

    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=13"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161115/hour=14"));
    verify(mockS3).exists(new Path(OUTPUT_PATH, "dateint=20161116/hour=13"));
    verify(mockS3).delete(
        new Path(OUTPUT_PATH, "dateint=20161116/hour=13"),
        true /* recursive */ );
    Assert.assertTrue("Should have aborted",
        ((TestPartitionedOutputCommitter) committer).aborted);
    verifyNoMoreInteractions(mockS3);
  }
}
