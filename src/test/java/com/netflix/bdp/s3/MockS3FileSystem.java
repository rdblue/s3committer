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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.mockito.Mockito;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public class MockS3FileSystem extends FileSystem {
  public static final String BUCKET = "bucket-name";
  public static final URI FS_URI = URI.create("s3://" + BUCKET + "/");

  private FileSystem mock = null;

  public MockS3FileSystem() {
  }

  @Override
  public String getScheme() {
    return FS_URI.getScheme();
  }

  @Override
  public URI getUri() {
    return FS_URI;
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path("s3://" + BUCKET + "/work");
  }

  public void setMock(FileSystem mock) {
    this.mock = mock;
  }

  @Override
  public boolean exists(Path f) throws IOException {
    return mock.exists(f);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return mock.open(f, bufferSize);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return mock.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return mock.append(f, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return mock.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return mock.delete(f, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    return mock.listStatus(f);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    mock.setWorkingDirectory(new_dir);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return mock.mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return mock.getFileStatus(f);
  }
}
