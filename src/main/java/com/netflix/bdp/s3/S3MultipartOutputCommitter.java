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
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.netflix.bdp.s3.Tasks.Task;
import com.netflix.bdp.s3.util.ConflictResolution;
import com.netflix.bdp.s3.util.HiddenPathFilter;
import com.netflix.bdp.s3.util.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Collections;

class S3MultipartOutputCommitter extends FileOutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(
      S3MultipartOutputCommitter.class);

  private final Path constructorOutputPath;
  private final long uploadPartSize;
  private final String uuid;
  private final Path workPath;
  private final FileOutputCommitter wrappedCommitter;

  // lazy variables
  private AmazonS3 client = null;
  private ConflictResolution mode = null;
  private ExecutorService threadPool = null;
  private Path finalOutputPath = null;
  private String bucket = null;
  private String s3KeyPrefix = null;
  private Path bucketRoot = null;

  public S3MultipartOutputCommitter(Path outputPath, JobContext context)
      throws IOException {
    super(outputPath, context);
    this.constructorOutputPath = outputPath;

    Configuration conf = context.getConfiguration();

    this.uploadPartSize = conf.getLong(
        S3Committer.UPLOAD_SIZE, S3Committer.DEFAULT_UPLOAD_SIZE);
    // Spark will use a fake app id based on the current minute and job id 0.
    // To avoid collisions, use the YARN application ID for Spark.
    this.uuid = conf.get(S3Committer.UPLOAD_UUID, conf.get(
        S3Committer.SPARK_WRITE_UUID,
        conf.get(S3Committer.SPARK_APP_ID, context.getJobID().toString())));

    if (context instanceof TaskAttemptContext) {
      this.workPath = taskAttemptPath((TaskAttemptContext) context, uuid);
    } else {
      this.workPath = null;
    }

    this.wrappedCommitter = new FileOutputCommitter(
        Paths.getMultipartUploadCommitsDirectory(conf, uuid), context);
  }

  public S3MultipartOutputCommitter(Path outputPath, TaskAttemptContext context)
      throws IOException {
    this(outputPath, (JobContext) context);
  }

  /**
   * Returns the target output path based on the output path passed to the
   * constructor.
   * <p>
   * Subclasses can override this method to redirect output. For example, a
   * committer can write output to a new directory instead of deleting the
   * current directory's contents, and then point a table to the new location.
   *
   * @param outputPath the output path passed to the constructor
   * @param context the JobContext passed to the constructor
   * @return the final output path
   */
  protected Path getFinalOutputPath(Path outputPath, JobContext context) {
    return outputPath;
  }

  /**
   * Returns an {@link AmazonS3} client. This should be overridden by
   * subclasses to provide access to a configured client.
   *
   * @param path the output S3 path (with bucket)
   * @param conf a Hadoop {@link Configuration}
   * @return a {@link AmazonS3} client
   */
  protected Object findClient(Path path, Configuration conf) {
    return new AmazonS3Client();
  }

  /**
   * Getter for the cached {@link AmazonS3} client. Subclasses should call this
   * method to get a client instance.
   *
   * @param path the output S3 path (with bucket)
   * @param conf a Hadoop {@link Configuration}
   * @return a {@link AmazonS3} client
   */
  protected AmazonS3 getClient(Path path, Configuration conf) {
    if (client != null) {
      return client;
    }

    Object found = findClient(path, conf);
    if (found instanceof AmazonS3) {
      this.client = (AmazonS3) found;
      return client;
    }

    if (found != null) {
      throw new RuntimeException("Failed to find a valid S3 client: " +
          found + " is not a " + AmazonS3.class.getName());
    }

    throw new RuntimeException("Failed to find a S3 client");
  }

  /**
   * Lists the output of a task under the task attempt path. Subclasses can
   * override this method to change how output files are identified.
   * <p>
   * This implementation lists the files that are direct children of the output
   * path and filters hidden files (file names starting with '.' or '_').
   * <p>
   * The task attempt path is provided by
   * {@link #getTaskAttemptPath(TaskAttemptContext)}
   *
   * @param context this task's {@link TaskAttemptContext}
   * @return the output files produced by this task in the task attempt path
   * @throws IOException
   */
  protected Iterable<FileStatus> getTaskOutput(TaskAttemptContext context)
      throws IOException {
    // get files on the local FS in the attempt path
    Path attemptPath = getTaskAttemptPath(context);
    FileSystem attemptFS = attemptPath.getFileSystem(context.getConfiguration());
    FileStatus[] stats = attemptFS.listStatus(
        attemptPath, HiddenPathFilter.get());
    return Arrays.asList(stats);
  }

  /**
   * Returns the final S3 key for a relative path. Subclasses can override this
   * method to upload files to a different S3 location.
   * <p>
   * This implementation concatenates the relative path with the key prefix
   * from the output path.
   *
   * @param relative the path of a file relative to the task attempt path
   * @param context the JobContext or TaskAttemptContext for this job
   * @return the S3 key where the file will be uploaded
   */
  protected String getFinalKey(String relative, JobContext context) {
    return getS3KeyPrefix(context) + "/" + Paths.addUUID(relative, uuid);
  }

  /**
   * Returns the final S3 location for a relative path as a Hadoop {@link Path}.
   * This is a final method that calls {@link #getFinalKey(String, JobContext)}
   * to determine the final location.
   *
   * @param relative the path of a file relative to the task attempt path
   * @param context the JobContext or TaskAttemptContext for this job
   * @return the S3 Path where the file will be uploaded
   */
  protected final Path getFinalPath(String relative, JobContext context) {
    return new Path(getBucketRoot(context), getFinalKey(relative, context));
  }

  @Override
  public Path getCommittedTaskPath(TaskAttemptContext context) {
    // return the location in HDFS where the multipart upload context is
    return wrappedCommitter.getCommittedTaskPath(context);
  }

  @Override
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    // a path on the local FS for files that will be uploaded
    return taskAttemptPath(context, uuid);
  }

  @Override
  public Path getJobAttemptPath(JobContext context) {
    return wrappedCommitter.getJobAttemptPath(context);
  }

  @Override
  public Path getWorkPath() throws IOException {
    return workPath;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    wrappedCommitter.setupJob(context);
    context.getConfiguration().set(S3Committer.UPLOAD_UUID, uuid);
  }

  protected List<S3Util.PendingUpload> getPendingUploads(JobContext context)
      throws IOException {
    return getPendingUploads(context, false);
  }

  protected List<S3Util.PendingUpload> getPendingUploadsIgnoreErrors(
      JobContext context) throws IOException {
    return getPendingUploads(context, true);
  }

  private List<S3Util.PendingUpload> getPendingUploads(
      JobContext context, boolean suppressExceptions) throws IOException {
    Path jobAttemptPath = wrappedCommitter.getJobAttemptPath(context);
    final FileSystem attemptFS = jobAttemptPath.getFileSystem(
        context.getConfiguration());
    FileStatus[] pendingCommitFiles = attemptFS.listStatus(
        jobAttemptPath, HiddenPathFilter.get());

    final List<S3Util.PendingUpload> pending =
        Collections.synchronizedList(new ArrayList<S3Util.PendingUpload>());

    // try to read every pending file and add all results to pending.
    // in the case of a failure to read the file, exceptions are held until all
    // reads have been attempted.
    Tasks.foreach(pendingCommitFiles)
        .throwFailureWhenFinished(!suppressExceptions)
        .executeWith(getThreadPool(context))
        .run(new Task<FileStatus, IOException>() {
          @Override
          public void run(FileStatus pendingCommitFile) throws IOException {
            pending.addAll(S3Util.readPendingCommits(
                attemptFS, pendingCommitFile.getPath()));
          }
        });

    return pending;
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    commitJobInternal(context, getPendingUploads(context));
  }

  protected void commitJobInternal(JobContext context,
                                   List<S3Util.PendingUpload> pending)
      throws IOException {
    final AmazonS3 client = getClient(
        getOutputPath(context), context.getConfiguration());

    boolean threw = true;
    try {
      Tasks.foreach(pending)
          .stopOnFailure().throwFailureWhenFinished()
          .executeWith(getThreadPool(context))
          .onFailure(new Tasks.FailureTask<S3Util.PendingUpload, RuntimeException>() {
            @Override
            public void run(S3Util.PendingUpload commit,
                            Exception exception) {
              S3Util.abortCommit(client, commit);
            }
          })
          .abortWith(new Task<S3Util.PendingUpload, RuntimeException>() {
            @Override
            public void run(S3Util.PendingUpload commit) {
              S3Util.abortCommit(client, commit);
            }
          })
          .revertWith(new Task<S3Util.PendingUpload, RuntimeException>() {
            @Override
            public void run(S3Util.PendingUpload commit) {
              S3Util.revertCommit(client, commit);
            }
          })
          .run(new Task<S3Util.PendingUpload, RuntimeException>() {
            @Override
            public void run(S3Util.PendingUpload commit) {
              S3Util.finishCommit(client, commit);
            }
          });

      threw = false;

    } finally {
      cleanup(context, threw);
    }
  }

  @Override
  public void abortJob(JobContext context, JobStatus.State state)
      throws IOException {
    List<S3Util.PendingUpload> pending = getPendingUploadsIgnoreErrors(context);
    abortJobInternal(context, pending, false);
  }

  protected void abortJobInternal(JobContext context,
                                  List<S3Util.PendingUpload> pending,
                                  boolean suppressExceptions)
      throws IOException {
    final AmazonS3 client = getClient(
        getOutputPath(context), context.getConfiguration());

    boolean threw = true;
    try {
      Tasks.foreach(pending)
          .throwFailureWhenFinished(!suppressExceptions)
          .executeWith(getThreadPool(context))
          .onFailure(new Tasks.FailureTask<S3Util.PendingUpload, RuntimeException>() {
            @Override
            public void run(S3Util.PendingUpload commit,
                            Exception exception) {
              S3Util.abortCommit(client, commit);
            }
          })
          .run(new Task<S3Util.PendingUpload, RuntimeException>() {
            @Override
            public void run(S3Util.PendingUpload commit) {
              S3Util.abortCommit(client, commit);
            }
          });

      threw = false;

    } finally {
      cleanup(context, threw || suppressExceptions);
    }
  }

  private void cleanup(JobContext context, boolean suppressExceptions)
      throws IOException {
    if (suppressExceptions) {
      try {
        wrappedCommitter.cleanupJob(context);
      } catch (Exception e) {
        LOG.error("Failed while cleaning up job", e);
      }
    } else {
      wrappedCommitter.cleanupJob(context);
    }
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    wrappedCommitter.setupTask(context);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context)
      throws IOException {
    // check for files on the local FS in the attempt path
    Path attemptPath = getTaskAttemptPath(context);
    FileSystem fs = attemptPath.getFileSystem(context.getConfiguration());

    if (fs.exists(attemptPath)) {
      FileStatus[] stats = fs.listStatus(attemptPath);
      return stats.length > 0;
    }

    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    Iterable<FileStatus> stats = getTaskOutput(context);
    commitTaskInternal(context, stats);
  }

  protected void commitTaskInternal(final TaskAttemptContext context,
                                    Iterable<FileStatus> taskOutput)
      throws IOException {
    Configuration conf = context.getConfiguration();
    final AmazonS3 client = getClient(getOutputPath(context), conf);

    final Path attemptPath = getTaskAttemptPath(context);
    FileSystem attemptFS = attemptPath.getFileSystem(conf);

    // add the commits file to the wrapped commiter's task attempt location.
    // this complete file will be committed by the wrapped committer at the end
    // of this method.
    Path commitsAttemptPath = wrappedCommitter.getTaskAttemptPath(context);
    FileSystem commitsFS = commitsAttemptPath.getFileSystem(conf);

    // keep track of unfinished commits in case one fails. if something fails,
    // we will try to abort the ones that had already succeeded.
    final List<S3Util.PendingUpload> commits =
        Collections.synchronizedList(new ArrayList<S3Util.PendingUpload>());

    boolean threw = true;
    ObjectOutputStream completeUploadRequests = new ObjectOutputStream(
        commitsFS.create(commitsAttemptPath, false));
    try {
      Tasks.foreach(taskOutput)
          .stopOnFailure().throwFailureWhenFinished()
          .executeWith(threadPool)
          .run(new Task<FileStatus, IOException>() {
            @Override
            public void run(FileStatus stat) throws IOException {
              File localFile = new File(
                  URI.create(stat.getPath().toString()).getPath());
              if (localFile.length() <= 0) {
                return;
              }
              String relative = Paths.getRelativePath(
                  attemptPath, stat.getPath());
              String partition = getPartition(relative);
              String key = getFinalKey(relative, context);
              S3Util.PendingUpload commit = S3Util.multipartUpload(client,
                  localFile, partition, getBucket(context), key,
                  uploadPartSize);
              commits.add(commit);
            }
          });

      for (S3Util.PendingUpload commit : commits) {
        completeUploadRequests.writeObject(commit);
      }

      threw = false;

    } finally {
      if (threw) {
        Tasks.foreach(commits)
            .run(new Task<S3Util.PendingUpload, RuntimeException>() {
              @Override
              public void run(S3Util.PendingUpload commit) {
                S3Util.abortCommit(client, commit);
              }
            });
        try {
          attemptFS.delete(attemptPath, true);
        } catch (Exception e) {
          LOG.error("Failed while cleaning up failed task commit: ", e);
        }
      }
      Closeables.close(completeUploadRequests, threw);
    }

    wrappedCommitter.commitTask(context);

    attemptFS.delete(attemptPath, true);
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    // the API specifies that the task has not yet been committed, so there are
    // no uploads that need to be cancelled. just delete files on the local FS.
    Path attemptPath = getTaskAttemptPath(context);
    FileSystem fs = attemptPath.getFileSystem(context.getConfiguration());
    if (!fs.delete(attemptPath, true)) {
      LOG.error("Failed to delete task attempt data: " + attemptPath);
    }
    wrappedCommitter.abortTask(context);
  }

  private static Path taskAttemptPath(TaskAttemptContext context, String uuid) {
    return getTaskAttemptPath(context, Paths.getLocalTaskAttemptTempDir(
        context.getConfiguration(), uuid,
        getTaskId(context), getAttemptId(context)));
  }

  private static int getTaskId(TaskAttemptContext context) {
    return context.getTaskAttemptID().getTaskID().getId();
  }

  private static int getAttemptId(TaskAttemptContext context) {
    return context.getTaskAttemptID().getId();
  }

  /**
   * Returns the partition of a relative file path, or null if the path is a
   * file name with no relative directory.
   *
   * @param relative a relative file path
   * @return the partition of the relative file path
   */
  protected final String getPartition(String relative) {
    return Paths.getParent(relative);
  }

  protected final Path getOutputPath(JobContext context) {
    if (finalOutputPath == null) {
      this.finalOutputPath = getFinalOutputPath(constructorOutputPath, context);
      Preconditions.checkNotNull(finalOutputPath, "Output path cannot be null");

      URI outputUri = URI.create(finalOutputPath.toString());
      Preconditions.checkArgument(outputUri.getScheme().startsWith("s3"),
          "Output path is not in S3: " + finalOutputPath);

      this.bucket = outputUri.getHost();
      this.s3KeyPrefix = Paths.removeStartingAndTrailingSlash(
          outputUri.getPath());
    }
    return finalOutputPath;
  }

  private final String getBucket(JobContext context) {
    if (bucket == null) {
      // getting the output path sets the bucket from the path
      getOutputPath(context);
    }
    return bucket;
  }

  private final String getS3KeyPrefix(JobContext context) {
    if (s3KeyPrefix == null) {
      // getting the output path sets the s3 key prefix from the path
      getOutputPath(context);
    }
    return s3KeyPrefix;
  }

  protected String getUUID() {
    return uuid;
  }

  /**
   * Returns an {@link ExecutorService} for parallel tasks. The number of
   * threads in the thread-pool is set by s3.multipart.committer.num-threads.
   * If num-threads is 0, this will return null;
   *
   * @param context the JobContext for this commit
   * @return an {@link ExecutorService} or null for the number of threads
   */
  protected final ExecutorService getThreadPool(JobContext context) {
    if (threadPool == null) {
      int numThreads = context.getConfiguration().getInt(
          S3Committer.NUM_THREADS, S3Committer.DEFAULT_NUM_THREADS);
      if (numThreads > 0) {
        this.threadPool = Executors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("s3-committer-pool-%d")
                .build());
      } else {
        return null;
      }
    }
    return threadPool;
  }

  /**
   * Returns the bucket root of the output path.
   *
   * @param context the JobContext for this commit
   * @return a Path that is the root of the output bucket
   */
  protected final Path getBucketRoot(JobContext context) {
    if (bucketRoot == null) {
      this.bucketRoot = Paths.getRoot(getOutputPath(context));
    }
    return bucketRoot;
  }

  /**
   * Returns the {@link ConflictResolution} mode for this commit.
   *
   * @param context the JobContext for this commit
   * @return the ConflictResolution mode
   */
  protected final ConflictResolution getMode(JobContext context) {
    if (mode == null) {
      this.mode = ConflictResolution.valueOf(context
          .getConfiguration()
          .get(S3Committer.CONFLICT_MODE, "fail")
          .toUpperCase(Locale.ENGLISH));
    }
    return mode;
  }
}
