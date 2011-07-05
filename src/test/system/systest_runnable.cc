// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/atomic.h"
#include "systest_runnable.h"
#include "systest_settings.h"

#include <errno.h>
#include <pthread.h>
#include <sstream>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/syscall.h>
#include <sys/types.h>
#include <vector>

using std::ostringstream;
using std::string;

static pid_t do_gettid(void)
{
  return static_cast < pid_t >(syscall(SYS_gettid));
}

ceph::atomic_t m_highest_id(0);

SysTestRunnable::
SysTestRunnable()
{
  m_started = false;
  m_id = m_highest_id.inc();
  memset(&m_pthread, 0, sizeof(m_pthread));
  m_pid = 0;
  update_id_str(false);
}

SysTestRunnable::
~SysTestRunnable()
{
}

const char* SysTestRunnable::
get_id_str(void) const
{
  return m_id_str;
}

int SysTestRunnable::
start()
{
  if (m_started) {
    return -EDOM;
  }
  bool use_threads = SysTestSettings::inst().use_threads();
  if (use_threads) {
    int ret = pthread_create(&m_pthread, NULL, systest_runnable_pthread_helper,
			     static_cast<void*>(this));
    if (ret)
      return ret;
    m_started = true;
    return 0;
  }
  else {
    // TODO: implement
    // m_pid = ???
    return -ENOTSUP;
  }
}

std::string SysTestRunnable::
join()
{
  if (!m_started) {
    return "SysTestRunnable was never started.";
  }
  bool use_threads = SysTestSettings::inst().use_threads();
  if (use_threads) {
    void *ptrretval;
    int ret = pthread_join(m_pthread, &ptrretval);
    if (ret) {
      ostringstream oss;
      oss << "pthread_join failed with error " << ret;
      return oss.str();
    }
    int retval = (int)(uintptr_t)ptrretval;
    if (retval != 0) {
      ostringstream oss;
      oss << "ERROR " << retval;
      return oss.str();
    }
    return "";
  }
  else {
    // TODO: implement
    // m_pid = ???
    return "processes not supported yet";
  }
}

void SysTestRunnable::
update_id_str(bool started)
{
  bool use_threads = SysTestSettings::inst().use_threads();
  char extra[128];
  extra[0] = '\0';

  if (started) {
    if (use_threads)
      snprintf(extra, sizeof(extra), " [%d]", do_gettid());
    else
      snprintf(extra, sizeof(extra), " [%d]", getpid());
  }
  if (use_threads)
    snprintf(m_id_str, SysTestRunnable::ID_STR_SZ, "thread %d%s", m_id, extra);
  else
    snprintf(m_id_str, SysTestRunnable::ID_STR_SZ, "process %d%s", m_id, extra);
}

std::string SysTestRunnable::
run_until_finished(std::vector < SysTestRunnable * > &runnables)
{
  int ret, index = 0;
  for (std::vector < SysTestRunnable * >::const_iterator r = runnables.begin();
      r != runnables.end(); ++r) {
    ret = (*r)->start();
    if (ret) {
      ostringstream oss;
      oss << "run_until_finished: got error " << ret
	  << " when starting runnable " << index;
      return oss.str();
    }
    ++index;
  }

  for (std::vector < SysTestRunnable * >::const_iterator r = runnables.begin();
      r != runnables.end(); ++r) {
    std::string rstr = (*r)->join();
    if (!rstr.empty()) {
      ostringstream oss;
      oss << "run_until_finished: runnable " << (*r)->get_id_str() 
	  << ": got error " << rstr;
      return oss.str();
    }
  }
  return "";
}

void *systest_runnable_pthread_helper(void *arg)
{
  SysTestRunnable *st = static_cast < SysTestRunnable * >(arg);
  st->update_id_str(true);
  int ret = st->run();
  return (void*)(uintptr_t)ret;
}
