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

public class S3Committer {
  public static final String UPLOAD_SIZE = "s3.multipart.committer.upload.size";
  public static final long DEFAULT_UPLOAD_SIZE = 10485760L; // 10 MB
  public static final String UPLOAD_UUID = "s3.multipart.committer.uuid";
  public static final String CONFLICT_MODE = "s3.multipart.committer.conflict-mode";
  public static final String NUM_THREADS = "s3.multipart.committer.num-threads";
  public static final int DEFAULT_NUM_THREADS = 8;

  // Spark configuration keys
  public static final String SPARK_WRITE_UUID = "spark.sql.sources.writeJobUUID";
  public static final String SPARK_APP_ID = "spark.app.id";
}
