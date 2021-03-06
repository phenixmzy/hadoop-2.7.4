/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Class that provides utility functions for checking disk problem
 */
/**
 * 该类提供了检测问题磁盘的工具方法,也就是提供了一个坏盘检测服务.
 * 检测的基本是每个磁盘,检测的对像是FsVolume,FsVolume对应一个存储数据的磁盘.
 * 通过检测文件目录的访问权限以及目录是否可以创建来判断所属磁盘的好坏,如果是坏盘,则此块盘将会被移除,盘上的所有块都将被重新复制.
 * */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DiskChecker {
  public static class DiskErrorException extends IOException {
    public DiskErrorException(String msg) {
      super(msg);
    }

    public DiskErrorException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
    
  public static class DiskOutOfSpaceException extends IOException {
    public DiskOutOfSpaceException(String msg) {
      super(msg);
    }
  }
      
  /** 
   * The semantics of mkdirsWithExistsCheck method is different from the mkdirs
   * method provided in the Sun's java.io.File class in the following way:
   * While creating the non-existent parent directories, this method checks for
   * the existence of those directories if the mkdir fails at any point (since
   * that directory might have just been created by some other process).
   * If both mkdir() and the exists() check fails for any seemingly 
   * non-existent directory, then we signal an error; Sun's mkdir would signal
   * an error (return false) if a directory it is attempting to create already
   * exists or the mkdir fails.
   * @param dir
   * @return true on success, false on failure
   */
  public static boolean mkdirsWithExistsCheck(File dir) {
    // 尝试创建目录并检查是否存在
    if (dir.mkdir() || dir.exists()) {
      return true;
    }
    File canonDir = null;
    try {
      // 如果失败,则往上一层检测,如果上一层的父目录已经不存在,则直接返回false
      canonDir = dir.getCanonicalFile();
    } catch (IOException e) {
      return false;
    }
    String parent = canonDir.getParent();
    return (parent != null) && 
           (mkdirsWithExistsCheck(new File(parent)) &&
                                      (canonDir.mkdir() || canonDir.exists()));
  }

  /**
   * Recurse down a directory tree, checking all child directories.
   * @param dir
   * @throws DiskErrorException
   */
  public static void checkDirs(File dir) throws DiskErrorException {
    checkDir(dir);
    for (File child : dir.listFiles()) {
      if (child.isDirectory()) {
        checkDirs(child);
      }
    }
  }
  
  /**
   * Create the directory if it doesn't exist and check that dir is readable,
   * writable and executable
   *  
   * @param dir
   * @throws DiskErrorException
   */
  public static void checkDir(File dir) throws DiskErrorException {
    // 尝试创建目录的检查
    if (!mkdirsWithExistsCheck(dir)) {
      throw new DiskErrorException("Cannot create directory: "
                                   + dir.toString());
    }
    // 文件目录的访问权限的检查
    checkDirAccess(dir);
  }

  /**
   * Create the directory or check permissions if it already exists.
   *
   * The semantics of mkdirsWithExistsAndPermissionCheck method is different
   * from the mkdirs method provided in the Sun's java.io.File class in the
   * following way:
   * While creating the non-existent parent directories, this method checks for
   * the existence of those directories if the mkdir fails at any point (since
   * that directory might have just been created by some other process).
   * If both mkdir() and the exists() check fails for any seemingly
   * non-existent directory, then we signal an error; Sun's mkdir would signal
   * an error (return false) if a directory it is attempting to create already
   * exists or the mkdir fails.
   *
   * @param localFS local filesystem
   * @param dir directory to be created or checked
   * @param expected expected permission
   * @throws IOException
   */
  public static void mkdirsWithExistsAndPermissionCheck(
      LocalFileSystem localFS, Path dir, FsPermission expected)
      throws IOException {
    File directory = localFS.pathToFile(dir);
    boolean created = false;

    if (!directory.exists())
      created = mkdirsWithExistsCheck(directory);

    if (created || !localFS.getFileStatus(dir).getPermission().equals(expected))
        localFS.setPermission(dir, expected);
  }

  /**
   * Create the local directory if necessary, check permissions and also ensure
   * it can be read from and written into.
   *
   * @param localFS local filesystem
   * @param dir directory
   * @param expected permission
   * @throws DiskErrorException
   * @throws IOException
   */
  public static void checkDir(LocalFileSystem localFS, Path dir,
                              FsPermission expected)
  throws DiskErrorException, IOException {
    mkdirsWithExistsAndPermissionCheck(localFS, dir, expected);
    checkDirAccess(localFS.pathToFile(dir));
  }

  /**
   * Checks that the given file is a directory and that the current running
   * process can read, write, and execute it.
   * 
   * @param dir File to check
   * @throws DiskErrorException if dir is not a directory, not readable, not
   *   writable, or not executable
   */
  private static void checkDirAccess(File dir) throws DiskErrorException {
    if (!dir.isDirectory()) {
      throw new DiskErrorException("Not a directory: "
                                   + dir.toString());
    }

    checkAccessByFileMethods(dir);
  }

  /**
   * Checks that the current running process can read, write, and execute the
   * given directory by using methods of the File object.
   * 
   * @param dir File to check
   * @throws DiskErrorException if dir is not readable, not writable, or not
   *   executable
   */
  private static void checkAccessByFileMethods(File dir)
      throws DiskErrorException {
    // 检查是否可读
    if (!FileUtil.canRead(dir)) {
      throw new DiskErrorException("Directory is not readable: "
                                   + dir.toString());
    }
    // 检查是否可写
    if (!FileUtil.canWrite(dir)) {
      throw new DiskErrorException("Directory is not writable: "
                                   + dir.toString());
    }
    // 检查是否可执行
    if (!FileUtil.canExecute(dir)) {
      throw new DiskErrorException("Directory is not executable: "
                                   + dir.toString());
    }
  }
}
