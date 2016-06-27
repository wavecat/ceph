// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_FUSESTORE_H
#define CEPH_OS_FUSESTORE_H

#include <mutex>

#include "common/Thread.h"
#include "os/ObjectStore.h"

class FuseStore {
public:
  ObjectStore *store;
  string mount_point;
  struct fs_info *info;
  std::mutex lock;

  struct OpenFile {
    string path;
    bufferlist bl;
    bool dirty = false;
    int ref = 0;
  };
  map<string,OpenFile*> open_files;

  int open_file(string p, struct fuse_file_info *fi,
		std::function<int(bufferlist *bl)> f);

  class FuseThread : public Thread {
    FuseStore *fs;
  public:
    explicit FuseThread(FuseStore *f) : fs(f) {}
    void *entry() {
      fs->loop();
      return NULL;
    }
  } fuse_thread;

  FuseStore(ObjectStore *s, string p);
  ~FuseStore();

  int main();
  int start();
  int loop();
  int stop();
};

#endif
