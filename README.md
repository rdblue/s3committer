# Iceberg

**I currently recommend using Iceberg tables instead of using these committers**. Check out the [Netflix/iceberg](https://github.com/Netflix/iceberg) project for details.


## S3 output committers

This project has Hadoop OutputCommitter implementations for S3.

There are 3 main classes:
* `S3MultipartOutputCommitter` is a base committer class that handles commit logic. This should not be used directly.
* `S3DirectoryOutputCommitter` for writing unpartitioned data to S3 with conflict resolution.
* `S3PartitionedOutputCommitter` for writing partitioned data to S3 with conflict resolution.

Callers should use `S3DirectoryOutputCommitter` for single-directory outputs, or `S3PartitionedOutputCommitter` for partitioned data.

### Commit logic

These S3 committers work by writing task outputs to a temporary directory on the local FS. Task outputs are directed to the local FS by `getTaskAttemptPath` and `getWorkPath`.

On task commit, the committers look for files in the task attempt directory (ignoring hidden files). Each file is uploaded to S3 using the [multi-part upload API][multi-part-upload-api], but the upload is *not completed in S3*. After task commit, files are not visible in S3. Instead the information for the upload is serialized to be completed during job commit. Information about a task's pending uploads is written to HDFS and committed using a `FileOutputCommitter`.

On job commit, the committer completes each multi-part upload started by tasks. This is safe because job commit only runs if all tasks completed successfully, and using the `FileOutputCommitter` to pass information to the job committer ensures that only one task attempt's output will be committed.

[multi-part-upload-api]: http://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html

### Conflict resolution

The single-directory and partitioned committers handle conflict resolution by checking whether target paths exist in S3 before uploading any data. There are 3 conflict resolution modes, controlled by setting `s3.multipart.committer.conflict-mode`:

* `fail`: Fail a task if an output directory or partition already exists. (Default)
* `append`: Upload data files without checking whether directories or partitions already exist.
* `replace`: If an output directory exists, delete it so the new data replaces the current content.

The partitioned committer enforces the conflict mode when a conflict is detected with output data, not before the job runs. Conflict resolution differs from an output mode because it does not enforce the mode when there is no conflict. For example, overwriting a partition should remove all sub-partitions and data it contains, whether or not new output is created. Conflict resolution will only replace partitions that have output data.

When the conflict mode is `replace`, conflicting directories are removed during job commit. Data is only deleted if all tasks have completed successfully.

A UUID that identifies a write is added to filenames that are uploaded to S3. This allows rolling back data from jobs that fail during job commit (see failure cases below) and avoids file-level conflicts when appending data to existing directories.

### Failure scenarios

Task commits delegate to the `FileOutputCommitter` to ensure that only one task's output reaches the job commit. If an upload fails, tasks will abort content already uploaded to S3 and will remove temporary files on the local FS. Similarly, if a task is aborted, temporary output on the local FS is removed.

If a task dies while the committer is running, it is possible for data to be left on the local FS or as unfinished parts in S3. Unfinished upload parts in S3 are not visible to table readers and are cleaned up following the rules in the target bucket's life-cycle policy.

Failures during job commit are handled by deleting any files that have already been completed and aborting the remaining uploads. Because uploads are completed individually, the files that are deleted were visible to readers.

If the process dies while the job committer is running, there are two possible failures:

1. Some directories that would be replaced have been deleted, but no new data is visible.
2. Some new data is visible but is not complete, and all replaced directories have been removed. Only complete files are visible.

If the process dies during job commit, cleaning up is a manual process. File names include a UUID for each write so that files can be identified and removed.

### Configuration

* `s3.multipart.committer.conflict-mode` -- how to resolve directory conflicts during commit: `fail`, `append`, or `replace`; defaults to `fail`.
* `s3.multipart.committer.uuid` -- a UUID that identifies a write; `spark.sql.sources.writeJobUUID` is used if not set
* `s3.multipart.committer.upload.size` -- size, in bytes, to use for parts of the upload to S3; defaults to 10MB.
* `s3.multipart.committer.num-threads` -- number of threads to use to complete S3 uploads during job commit; defaults to 8.

## Build

This project uses gradle. To build and test, run `gradle build`.
