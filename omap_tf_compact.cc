// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdint.h>
#include <string>
#include <tuple>
#include <vector>
#include <thread>
#include <iostream>
#include <boost/program_options.hpp>
#include <rados/librados.hpp>

namespace {

  // key count
  // key template

  std::vector<std::thread> thrds;

  std::string ceph_conf{"/opt/ceph-rgw/etc/ceph/ceph.conf"};
  std::string userid{""}; // e.g., admin
  std::string pool{"carlos-danger"};
  std::string object{"myobject"};
  uint64_t n_objects = 100000;

  class RadosCTX
  {
  public:
    librados::Rados rados;
    bool initialized;

    RadosCTX() : initialized(false) {
      int ret = rados.init(userid.c_str());
      if (ret < 0) {
	std::cout << "Rados::init failed" << std::endl;
	return;
      }
      ret = rados.conf_read_file(ceph_conf.c_str());
      if (ret < 0) {
	std::cout << "failed to read ceph_conf" << std::endl;
	return;
      }

      ret = rados.connect();
      if (ret < 0) {
	std::cout << "rados_connect failed" << std::endl;
	return;
      }
      ret = rados.pool_create(pool.c_str());
      initialized = true;
    }

    ~RadosCTX() {
      rados.shutdown();
    }
  }; /* RadosCTX */

  class ObjKeySeq
  {
  public:
    std::string s1 = "08b911c5-a313-4c06-a46d-451d064c6570.4100.";
    uint64_t ctr;
    std::string s2 =
      "__multipart_my-multipart-key-1.2~l423STlG8bMdwMMCIW-AWzwCZ8wlX92.meta";

    ObjKeySeq() : ctr() {}

    std::string next_key() {
      std::string key;
      key.reserve(s1.length() + s2.length() + 64);
      key.append(s1);
      key.append(std::to_string(++ctr));
      key.append(s2);
      return key;
    }
  };

  class InsertRGWKeys
  {
  public:
    RadosCTX& rctx;
    librados::IoCtx io_ctx;

    InsertRGWKeys(RadosCTX& _rctx) : rctx(_rctx) {
      int ret = rctx.rados.ioctx_create(pool.c_str(), io_ctx);
      if (ret < 0) {
	std::cout << "rados_ioctx_create failed " << ret << std::endl;
      }
      ceph::buffer::list bl;
      bl.push_back(
	ceph::buffer::create_static(7, const_cast<char*>("<nihil>")));
      ret = io_ctx.write_full(object, bl);
    }

    void operator()()
    {
      ObjKeySeq seq;
      std::string val{"now is the time for all good beings"};
      ceph::buffer::list bl;
      bl.append(val);
      for(int i = 0; i < n_objects; i++) {
	std::string key = seq.next_key();
	std::map<std::string, ceph::buffer::list> kmap;
	kmap.insert(
	  std::map<std::string, ceph::buffer::list>::value_type(key, bl));
	int ret = io_ctx.omap_set(object, kmap);
	if (ret >= 0) {
	  std::cout << "inserted: key " << key <<std::endl;
	}
      }
    }

    ~InsertRGWKeys() {}

  }; /* InsertRGWKeys */

  class ReadRGWKeys
  {
  public:
    RadosCTX& rctx;
    librados::IoCtx io_ctx;
    uint64_t nread;

    ReadRGWKeys(RadosCTX& _rctx) : rctx(_rctx), nread(0) {
      int ret = rctx.rados.ioctx_create(pool.c_str(), io_ctx);
      if (ret < 0) {
	std::cout << "rados_ioctx_create failed " << ret << std::endl;
      }
      ceph::buffer::list bl;
      bl.push_back(
	ceph::buffer::create_static(7, const_cast<char*>("<nihil>")));
      ret = io_ctx.write_full(object, bl);
    }

    void operator()()
    {
      bool more = true;
      uint64_t max = 1024;
      std::set<std::string> keys; // failing NYT!
      std::string marker;
      std::string &rmarker = marker;
      do {
	keys.clear();
	rmarker = marker;
	int ret = io_ctx.omap_get_keys2(object, rmarker, max, &keys, &more);
	for (auto& k : keys) {
	  std::cout << "\tkey: " << k << std::endl;
	  rmarker = k;
	  ++nread;
	}
      } while (more);
      std::cout << "read " << nread << " keys" << std::endl;
    }

    ~ReadRGWKeys() {}

  }; /* ReadRGWKeys */

  class ClearRGWKeys
  {
  public:
    RadosCTX& rctx;
    librados::IoCtx io_ctx;

    ClearRGWKeys(RadosCTX& _rctx) : rctx(_rctx) {
      int ret = rctx.rados.ioctx_create(pool.c_str(), io_ctx);
      if (ret < 0) {
	std::cout << "rados_ioctx_create failed " << ret << std::endl;
      }
    }

    void operator()()
    {
      int ret = io_ctx.remove(object);
      if (ret < 0) {
	std::cout << "rados.remove failed " << ret << std::endl;
      }
    }

    ~ClearRGWKeys() {}

  }; /* ClearRGWKeys */

} /* namespace */


int main(int argc, char *argv[])
{
  namespace po = boost::program_options;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("get", "get existing keys")
    ("clear", "clear keys")
    ("set", "set keys")
    ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  RadosCTX rctx;
  if (vm.count("get")) {
    thrds.push_back(std::thread(ReadRGWKeys(rctx)));
  } else if (vm.count("clear")) {
    thrds.push_back(std::thread(ClearRGWKeys(rctx)));
  } else{
    thrds.push_back(std::thread(InsertRGWKeys(rctx)));
  }

  for (auto& thrd : thrds) {
    thrd.join();
  }

  return 0;
}