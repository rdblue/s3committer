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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.netflix.bdp.s3.util.Paths;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class S3Util {

  private static final Logger LOG = LoggerFactory.getLogger(S3Util.class);

  public static void revertCommit(AmazonS3 client,
                                  PendingUpload commit) {
    client.deleteObject(commit.newDeleteRequest());
  }

  public static void finishCommit(AmazonS3 client,
                                  PendingUpload commit) {
    client.completeMultipartUpload(commit.newCompleteRequest());
  }

  public static void abortCommit(AmazonS3 client,
                                 PendingUpload commit) {
    client.abortMultipartUpload(commit.newAbortRequest());
  }

  public static PendingUpload multipartUpload(
      AmazonS3 client, File localFile, String partition,
      String bucket, String key, long uploadPartSize) {

    InitiateMultipartUploadResult initiate = client.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(bucket, key));
    String uploadId = initiate.getUploadId();

    boolean threw = true;
    try {
      Map<Integer, String> etags = Maps.newLinkedHashMap();

      long offset = 0;
      long numParts = (localFile.length() / uploadPartSize +
          ((localFile.length() % uploadPartSize) > 0 ? 1 : 0));

      Preconditions.checkArgument(numParts > 0,
          "Cannot upload 0 byte file: " + localFile);

      for (int partNumber = 1; partNumber <= numParts; partNumber += 1) {
        long size = Math.min(localFile.length() - offset, uploadPartSize);
        UploadPartRequest part = new UploadPartRequest()
            .withBucketName(bucket)
            .withKey(key)
            .withPartNumber(partNumber)
            .withUploadId(uploadId)
            .withFile(localFile)
            .withFileOffset(offset)
            .withPartSize(size)
            .withLastPart(partNumber == numParts);

        UploadPartResult partResult = client.uploadPart(part);
        PartETag etag = partResult.getPartETag();
        etags.put(etag.getPartNumber(), etag.getETag());

        offset += uploadPartSize;
      }

      PendingUpload pending = new PendingUpload(
          partition, bucket, key, uploadId, etags);

      threw = false;

      return pending;

    } finally {
      if (threw) {
        try {
          client.abortMultipartUpload(
              new AbortMultipartUploadRequest(bucket, key, uploadId));
        } catch (AmazonClientException e) {
          LOG.error("Failed to abort multi-part upload", e);
        }
      }
    }
  }

  static List<PendingUpload> readPendingCommits(FileSystem fs,
                                                Path pendingCommitsFile)
      throws IOException {
    List<PendingUpload> commits = Lists.newArrayList();

    ObjectInputStream in = new ObjectInputStream(fs.open(pendingCommitsFile));
    boolean threw = true;
    try {
      for (PendingUpload commit : new ObjectIterator<PendingUpload>(in)) {
        commits.add(commit);
      }
      threw = false;
    } finally {
      Closeables.close(in, threw);
    }

    return commits;
  }

  private static class ObjectIterator<T> implements Iterator<T>, Iterable<T> {
    private final ObjectInputStream stream;
    private boolean hasNext;
    private T next;

    public ObjectIterator(ObjectInputStream stream) {
      this.stream = stream;
      readNext();
    }

    @Override
    public Iterator<T> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public T next() {
      T toReturn = next;
      this.readNext();
      return toReturn;
    }

    @SuppressWarnings("unchecked")
    public void readNext() {
      try {
        this.next = (T) stream.readObject();
        this.hasNext = (next != null);
      } catch (EOFException e) {
        this.hasNext = false;
      } catch (ClassNotFoundException | IOException e) {
        this.hasNext = false;
        Throwables.propagate(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not implemented");
    }
  }

  /**
   * This class is used to pass information about pending uploads from tasks to
   * the job committer. It is serializable and will instantiate S3 requests.
   */
  public static class PendingUpload implements Serializable {
    private final String partition;
    private final String bucket;
    private final String key;
    private final String uploadId;
    private final SortedMap<Integer, String> parts;

    public PendingUpload(String partition, String bucket, String key,
                         String uploadId, Map<Integer, String> etags) {
      this.partition = partition;
      this.bucket = bucket;
      this.key = key;
      this.uploadId = uploadId;
      this.parts = ImmutableSortedMap.copyOf(etags);
    }

    public PendingUpload(String bucket, String key,
                         String uploadId, List<PartETag> etags) {
      this.partition = null;
      this.bucket = bucket;
      this.key = key;
      this.uploadId = uploadId;

      ImmutableSortedMap.Builder<Integer, String> builder =
          ImmutableSortedMap.builder();
      for (PartETag etag : etags) {
        builder.put(etag.getPartNumber(), etag.getETag());
      }

      this.parts = builder.build();
    }

    public CompleteMultipartUploadRequest newCompleteRequest() {
      List<PartETag> etags = Lists.newArrayList();
      for (Map.Entry<Integer, String> entry : parts.entrySet()) {
        etags.add(new PartETag(entry.getKey(), entry.getValue()));
      }

      return new CompleteMultipartUploadRequest(
          bucket, key, uploadId, etags);
    }

    public DeleteObjectRequest newDeleteRequest() {
      return new DeleteObjectRequest(bucket, key);
    }

    public AbortMultipartUploadRequest newAbortRequest() {
      return new AbortMultipartUploadRequest(bucket, key, uploadId);
    }

    public String getBucketName() {
      return bucket;
    }

    public String getKey() {
      return key;
    }

    public String getUploadId() {
      return uploadId;
    }

    public SortedMap<Integer, String> getParts() {
      return parts;
    }

    public String getPartition() {
      return partition;
    }

    public String getLocation() {
      return Paths.getParent(key);
    }
  }
}
