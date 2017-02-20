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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.netflix.bdp.s3.S3Committer.UPLOAD_SIZE;
import static com.netflix.bdp.s3.S3Committer.UPLOAD_UUID;


@RunWith(Parameterized.class)
public class TestS3MultipartOutputCommitter extends TestUtil.MiniDFSTest {

  private static final JobID JOB_ID = new JobID("job", 1);
  private static final TaskAttemptID AID = new TaskAttemptID(
      new TaskID(JOB_ID, TaskType.REDUCE, 2), 3);

  private static final String BUCKET = "bucket-name";
  private static final String KEY_PREFIX = "output/path";
  private static Path S3_OUTPUT_PATH = null;

  private final int numThreads;
  private JobContext job = null;
  private String uuid = null;
  private TaskAttemptContext tac = null;
  private Configuration conf = null;
  private MockedS3Committer jobCommitter = null;
  private MockedS3Committer committer = null;

  @BeforeClass
  public static void setupS3() {
    getConfiguration().set("fs.s3.impl", MockS3FileSystem.class.getName());
    S3_OUTPUT_PATH = new Path("s3://" + BUCKET + "/" + KEY_PREFIX);
  }

  @Parameterized.Parameters
  public static Object[][] params() {
    return new Object[][] {
        new Object[] { 0 },
        new Object[] { 1 },
        new Object[] { 3 } };
  }

  public TestS3MultipartOutputCommitter(int numThreads) {
    this.numThreads = numThreads;
  }

  @Before
  public void setupCommitter() throws Exception {
    getConfiguration().set(
        "s3.multipart.committer.num-threads", String.valueOf(numThreads));
    getConfiguration().set(UPLOAD_UUID, UUID.randomUUID().toString());
    this.job = new JobContextImpl(getConfiguration(), JOB_ID);
    this.jobCommitter = new MockedS3Committer(S3_OUTPUT_PATH, job);
    jobCommitter.setupJob(job);
    this.uuid = job.getConfiguration().get(UPLOAD_UUID);

    this.tac = new TaskAttemptContextImpl(
        new Configuration(job.getConfiguration()), AID);

    // get the task's configuration copy so modifications take effect
    this.conf = tac.getConfiguration();
    conf.set("mapred.local.dir", "/tmp/local-0,/tmp/local-1");
    conf.setInt(UPLOAD_SIZE, 100);

    this.committer = new MockedS3Committer(S3_OUTPUT_PATH, tac);
  }

  @Test
  public void testAttemptPathConstruction() throws Exception {
    // the temp directory is chosen based on a random seeded by the task and
    // attempt ids, so the result is deterministic if those ids are fixed.
    conf.set("mapred.local.dir", "/tmp/mr-local-0,/tmp/mr-local-1");

    Assert.assertEquals("Missing scheme should produce local file paths",
        "file:/tmp/mr-local-1/" + uuid + "/_temporary/0/_temporary/attempt_job_0001_r_000002_3",
        committer.getTaskAttemptPath(tac).toString());

    conf.set("mapred.local.dir", "file:/tmp/mr-local-0,file:/tmp/mr-local-1");
    Assert.assertEquals("Path should be the same with file scheme",
        "file:/tmp/mr-local-1/" + uuid + "/_temporary/0/_temporary/attempt_job_0001_r_000002_3",
        committer.getTaskAttemptPath(tac).toString());

    conf.set("mapred.local.dir",
        "hdfs://nn:8020/tmp/mr-local-0,hdfs://nn:8020/tmp/mr-local-1");
    TestUtil.assertThrows("Should not allow temporary storage in HDFS",
        IllegalArgumentException.class, "Wrong FS",
        new Runnable() {
          @Override
          public void run() {
            committer.getTaskAttemptPath(tac);
          }
        });
  }

  @Test
  public void testCommitPathConstruction() throws Exception {
    Path expected = getDFS().makeQualified(new Path(
        "hdfs:/tmp/" + uuid + "/pending-uploads/_temporary/0/task_job_0001_r_000002"));
    Assert.assertEquals("Path should be in HDFS",
        expected, committer.getCommittedTaskPath(tac));
  }

  @Test
  public void testSingleTaskCommit() throws Exception {
    Path file = new Path(commitTask(committer, tac, 1).iterator().next());

    List<String> uploads = committer.results.getUploads();
    Assert.assertEquals("Should initiate one upload", 1, uploads.size());

    Path committedPath = committer.getCommittedTaskPath(tac);
    FileSystem dfs = committedPath.getFileSystem(conf);

    Assert.assertEquals("Should commit to HDFS", getDFS(), dfs);

    FileStatus[] stats = dfs.listStatus(committedPath);
    Assert.assertEquals("Should produce one commit file", 1, stats.length);
    Assert.assertEquals("Should name the commits file with the task ID",
        "task_job_0001_r_000002", stats[0].getPath().getName());

    List<S3Util.PendingUpload> pending = S3Util.
        readPendingCommits(dfs, stats[0].getPath());
    Assert.assertEquals("Should have one pending commit", 1, pending.size());

    S3Util.PendingUpload commit = pending.get(0);
    Assert.assertEquals("Should write to the correct bucket",
        BUCKET, commit.getBucketName());
    Assert.assertEquals("Should write to the correct key",
        KEY_PREFIX + "/" + file.getName(), commit.getKey());

    assertValidUpload(committer.results.getTagsByUpload(), commit);
  }

  @Test
  public void testSingleTaskEmptyFileCommit() throws Exception {
    committer.setupTask(tac);

    Path attemptPath = committer.getTaskAttemptPath(tac);

    String rand = UUID.randomUUID().toString();
    writeOutputFile(tac.getTaskAttemptID(), attemptPath, rand, 0);

    committer.commitTask(tac);

    List<String> uploads = committer.results.getUploads();
    Assert.assertEquals("Should initiate one upload", 0, uploads.size());

    Path committedPath = committer.getCommittedTaskPath(tac);
    FileSystem dfs = committedPath.getFileSystem(conf);

    Assert.assertEquals("Should commit to HDFS", getDFS(), dfs);

    FileStatus[] stats = dfs.listStatus(committedPath);
    Assert.assertEquals("Should produce one commit file", 1, stats.length);
    Assert.assertEquals("Should name the commits file with the task ID",
        "task_job_0001_r_000002", stats[0].getPath().getName());

    List<S3Util.PendingUpload> pending = S3Util.
        readPendingCommits(dfs, stats[0].getPath());
    Assert.assertEquals("Should have no pending commits", 0, pending.size());
  }

  @Test
  public void testSingleTaskMultiFileCommit() throws Exception {
    int numFiles = 3;
    Set<String> files = commitTask(committer, tac, numFiles);

    List<String> uploads = committer.results.getUploads();
    Assert.assertEquals("Should initiate multiple uploads", numFiles, uploads.size());

    Path committedPath = committer.getCommittedTaskPath(tac);
    FileSystem dfs = committedPath.getFileSystem(conf);

    Assert.assertEquals("Should commit to HDFS", getDFS(), dfs);

    FileStatus[] stats = dfs.listStatus(committedPath);
    Assert.assertEquals("Should produce one commit file", 1, stats.length);
    Assert.assertEquals("Should name the commits file with the task ID",
        "task_job_0001_r_000002", stats[0].getPath().getName());

    List<S3Util.PendingUpload> pending = S3Util.
        readPendingCommits(dfs, stats[0].getPath());
    Assert.assertEquals("Should have correct number of pending commits",
        files.size(), pending.size());

    Set<String> keys = Sets.newHashSet();
    for (S3Util.PendingUpload commit : pending) {
      Assert.assertEquals("Should write to the correct bucket",
          BUCKET, commit.getBucketName());
      assertValidUpload(committer.results.getTagsByUpload(), commit);
      keys.add(commit.getKey());
    }

    Assert.assertEquals("Should write to the correct key",
        files, keys);
  }

  @Test
  public void testTaskInitializeFailure() throws Exception {
    committer.setupTask(tac);

    committer.errors.failOnInit(1);

    Path attemptPath = committer.getTaskAttemptPath(tac);
    FileSystem fs = attemptPath.getFileSystem(conf);

    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);
    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);

    TestUtil.assertThrows("Should fail during init",
        AmazonClientException.class, "Fail on init 1",
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            committer.commitTask(tac);
            return null;
          }
        });

    Assert.assertEquals("Should have initialized one file upload",
        1, committer.results.getUploads().size());
    Assert.assertEquals("Should abort the upload",
        new HashSet<>(committer.results.getUploads()),
        getAbortedIds(committer.results.getAborts()));
    Assert.assertFalse("Should remove the attempt path",
        fs.exists(attemptPath));
  }

  @Test
  public void testTaskSingleFileUploadFailure() throws Exception {
    committer.setupTask(tac);

    committer.errors.failOnUpload(2);

    Path attemptPath = committer.getTaskAttemptPath(tac);
    FileSystem fs = attemptPath.getFileSystem(conf);

    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);

    TestUtil.assertThrows("Should fail during upload",
        AmazonClientException.class, "Fail on upload 2",
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            committer.commitTask(tac);
            return null;
          }
        });

    Assert.assertEquals("Should have attempted one file upload",
        1, committer.results.getUploads().size());
    Assert.assertEquals("Should abort the upload",
        committer.results.getUploads().get(0),
        committer.results.getAborts().get(0).getUploadId());
    Assert.assertFalse("Should remove the attempt path",
        fs.exists(attemptPath));
  }

  @Test
  public void testTaskMultiFileUploadFailure() throws Exception {
    committer.setupTask(tac);

    committer.errors.failOnUpload(5);

    Path attemptPath = committer.getTaskAttemptPath(tac);
    FileSystem fs = attemptPath.getFileSystem(conf);

    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);
    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);

    TestUtil.assertThrows("Should fail during upload",
        AmazonClientException.class, "Fail on upload 5",
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            committer.commitTask(tac);
            return null;
          }
        });

    Assert.assertEquals("Should have attempted two file uploads",
        2, committer.results.getUploads().size());
    Assert.assertEquals("Should abort the upload",
        new HashSet<>(committer.results.getUploads()),
        getAbortedIds(committer.results.getAborts()));
    Assert.assertFalse("Should remove the attempt path",
        fs.exists(attemptPath));
  }

  @Test
  public void testTaskUploadAndAbortFailure() throws Exception {
    committer.setupTask(tac);

    committer.errors.failOnUpload(5);
    committer.errors.failOnAbort(0);

    Path attemptPath = committer.getTaskAttemptPath(tac);
    FileSystem fs = attemptPath.getFileSystem(conf);

    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);
    writeOutputFile(tac.getTaskAttemptID(), attemptPath,
        UUID.randomUUID().toString(), 10);

    TestUtil.assertThrows(
        "Should suppress abort failure, propagate upload failure",
        AmazonClientException.class, "Fail on upload 5",
        new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            committer.commitTask(tac);
            return null;
          }
        });

    Assert.assertEquals("Should have attempted two file uploads",
        2, committer.results.getUploads().size());
    Assert.assertEquals("Should not have succeeded with any aborts",
        new HashSet<>(),
        getAbortedIds(committer.results.getAborts()));
    Assert.assertFalse("Should remove the attempt path",
        fs.exists(attemptPath));
  }

  @Test
  public void testSingleTaskAbort() throws Exception {
    committer.setupTask(tac);

    Path attemptPath = committer.getTaskAttemptPath(tac);
    FileSystem fs = attemptPath.getFileSystem(conf);

    Path outPath = writeOutputFile(
        tac.getTaskAttemptID(), attemptPath, UUID.randomUUID().toString(), 10);

    committer.abortTask(tac);

    Assert.assertEquals("Should not upload anything",
        0, committer.results.getUploads().size());
    Assert.assertEquals("Should not upload anything",
        0, committer.results.getParts().size());
    Assert.assertFalse("Should remove all attempt data",
        fs.exists(outPath));
    Assert.assertFalse("Should remove the attempt path",
        fs.exists(attemptPath));
  }

  @Test
  public void testJobCommit() throws Exception {
    Path jobAttemptPath = jobCommitter.getJobAttemptPath(job);
    FileSystem fs = jobAttemptPath.getFileSystem(conf);

    Set<String> uploads = runTasks(job, 4, 3);

    Assert.assertTrue(fs.exists(jobAttemptPath));

    jobCommitter.commitJob(job);
    Assert.assertEquals("Should have aborted no uploads",
        0, jobCommitter.results.getAborts().size());

    Assert.assertEquals("Should have deleted no uploads",
        0, jobCommitter.results.getDeletes().size());

    Assert.assertEquals("Should have committed all uploads",
        uploads, getCommittedIds(jobCommitter.results.getCommits()));

    Assert.assertFalse(fs.exists(jobAttemptPath));
  }

  @Test
  public void testJobCommitFailure() throws Exception {
    Path jobAttemptPath = jobCommitter.getJobAttemptPath(job);
    FileSystem fs = jobAttemptPath.getFileSystem(conf);

    Set<String> uploads = runTasks(job, 4, 3);

    Assert.assertTrue(fs.exists(jobAttemptPath));

    jobCommitter.errors.failOnCommit(5);

    TestUtil.assertThrows("Should propagate the commit failure",
        AmazonClientException.class, "Fail on commit 5", new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            jobCommitter.commitJob(job);
            return null;
          }
        });

    Assert.assertEquals("Should have succeeded to commit some uploads",
        5, jobCommitter.results.getCommits().size());

    Assert.assertEquals("Should have deleted the files that succeeded",
        5, jobCommitter.results.getDeletes().size());

    Set<String> commits = Sets.newHashSet();
    for (CompleteMultipartUploadRequest commit : jobCommitter.results.getCommits()) {
      commits.add(commit.getBucketName() + commit.getKey());
    }

    Set<String> deletes = Sets.newHashSet();
    for (DeleteObjectRequest delete : jobCommitter.results.getDeletes()) {
      deletes.add(delete.getBucketName() + delete.getKey());
    }

    Assert.assertEquals("Committed and deleted objects should match",
        commits, deletes);

    Assert.assertEquals("Should have aborted the remaining uploads",
        7, jobCommitter.results.getAborts().size());

    Set<String> uploadIds = getCommittedIds(jobCommitter.results.getCommits());
    uploadIds.addAll(getAbortedIds(jobCommitter.results.getAborts()));

    Assert.assertEquals("Should have committed/deleted or aborted all uploads",
        uploads, uploadIds);

    Assert.assertFalse(fs.exists(jobAttemptPath));
  }

  @Test
  public void testJobAbortFailure() throws Exception {
    Path jobAttemptPath = jobCommitter.getJobAttemptPath(job);
    FileSystem fs = jobAttemptPath.getFileSystem(conf);

    Set<String> uploads = runTasks(job, 4, 3);

    Assert.assertTrue(fs.exists(jobAttemptPath));

    jobCommitter.errors.failOnAbort(5);
    jobCommitter.errors.recoverAfterFailure();

    TestUtil.assertThrows("Should propagate the abort failure",
        AmazonClientException.class, "Fail on abort 5", new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            jobCommitter.abortJob(job, JobStatus.State.KILLED);
            return null;
          }
        });

    Assert.assertEquals("Should not have committed any uploads",
        0, jobCommitter.results.getCommits().size());

    Assert.assertEquals("Should have deleted no uploads",
        0, jobCommitter.results.getDeletes().size());

    Assert.assertEquals("Should have aborted all uploads",
        12, jobCommitter.results.getAborts().size());

    Set<String> uploadIds = getCommittedIds(jobCommitter.results.getCommits());
    uploadIds.addAll(getAbortedIds(jobCommitter.results.getAborts()));

    Assert.assertEquals("Should have committed or aborted all uploads",
        uploads, uploadIds);

    Assert.assertFalse(fs.exists(jobAttemptPath));
  }

  @Test
  public void testJobAbort() throws Exception {
    Path jobAttemptPath = jobCommitter.getJobAttemptPath(job);
    FileSystem fs = jobAttemptPath.getFileSystem(conf);

    Set<String> uploads = runTasks(job, 4, 3);

    Assert.assertTrue(fs.exists(jobAttemptPath));

    jobCommitter.abortJob(job, JobStatus.State.KILLED);
    Assert.assertEquals("Should have committed no uploads",
        0, jobCommitter.results.getCommits().size());

    Assert.assertEquals("Should have deleted no uploads",
        0, jobCommitter.results.getDeletes().size());

    Assert.assertEquals("Should have aborted all uploads",
        uploads, getAbortedIds(jobCommitter.results.getAborts()));

    Assert.assertFalse(fs.exists(jobAttemptPath));
  }

  private static Set<String> runTasks(JobContext job, int numTasks, int numFiles)
      throws IOException {
    Set<String> uploads = Sets.newHashSet();

    for (int taskId = 0; taskId < numTasks; taskId += 1) {
      TaskAttemptID attemptID = new TaskAttemptID(
          new TaskID(JOB_ID, TaskType.REDUCE, taskId),
          (taskId * 37) % numTasks);
      TaskAttemptContext attempt = new TaskAttemptContextImpl(
          new Configuration(job.getConfiguration()), attemptID);
      MockedS3Committer taskCommitter = new MockedS3Committer(
          S3_OUTPUT_PATH, attempt);
      commitTask(taskCommitter, attempt, numFiles);
      uploads.addAll(taskCommitter.results.getUploads());
    }

    return uploads;
  }

  private static Set<String> getAbortedIds(
      List<AbortMultipartUploadRequest> aborts) {
    Set<String> abortedUploads = Sets.newHashSet();
    for (AbortMultipartUploadRequest abort : aborts) {
      abortedUploads.add(abort.getUploadId());
    }
    return abortedUploads;
  }

  private static Set<String> getCommittedIds(
      List<CompleteMultipartUploadRequest> commits) {
    Set<String> committedUploads = Sets.newHashSet();
    for (CompleteMultipartUploadRequest commit : commits) {
      committedUploads.add(commit.getUploadId());
    }
    return committedUploads;
  }

  private static Set<String> commitTask(S3MultipartOutputCommitter committer,
                                        TaskAttemptContext tac, int numFiles)
      throws IOException {
    Path attemptPath = committer.getTaskAttemptPath(tac);

    Set<String> files = Sets.newHashSet();
    for (int i = 0; i < numFiles; i += 1) {
      Path outPath = writeOutputFile(
          tac.getTaskAttemptID(), attemptPath, UUID.randomUUID().toString(),
          10 * (i+1));
      files.add(KEY_PREFIX +
          "/" + outPath.getName() + "-" + committer.getUUID());
    }

    committer.commitTask(tac);

    return files;
  }

  private static void assertValidUpload(Map<String, List<String>> parts,
                                        S3Util.PendingUpload commit) {
    Assert.assertTrue("Should commit a valid uploadId",
        parts.containsKey(commit.getUploadId()));

    List<String> tags = parts.get(commit.getUploadId());
    Assert.assertEquals("Should commit the correct number of file parts",
        tags.size(), commit.getParts().size());

    for (int i = 0; i < tags.size(); i += 1) {
      Assert.assertEquals("Should commit the correct part tags",
          tags.get(i), commit.getParts().get(i + 1));
    }
  }

  private static Path writeOutputFile(TaskAttemptID id, Path dest,
                                      String content, long copies)
      throws IOException {
    String fileName = ((id.getTaskType() == TaskType.REDUCE) ? "r_" : "m_") +
        id.getTaskID().getId() + "_" + id.getId() + "_" +
        UUID.randomUUID().toString();
    Path outPath = new Path(dest, fileName);
    FileSystem fs = outPath.getFileSystem(getConfiguration());

    try (OutputStream out = fs.create(outPath)) {
      byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
      for (int i = 0; i < copies; i += 1) {
        out.write(bytes);
      }
    }

    return outPath;
  }
}
