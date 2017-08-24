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
#include <chrono>

namespace {

  std::vector<std::thread> thrds;
  
  std::string ceph_conf{"/etc/ceph/ceph.conf"};
  std::string userid{"admin"}; // e.g., admin
  std::string pool{"mypool"};
  std::string default_object{"myobject"};
  uint64_t n_keys = 100;
  uint64_t n_objects = 100; // kali
  uint32_t n_threads = 1;
  uint32_t val_size = 200;
  bool verbose = false;

  enum class Adhoc : uint16_t {
    OP_GET = 0,
    OP_SET,
    OP_CLEAR
  };

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


  class OmapVal
  {
  public:
    std::string data;
    OmapVal(uint32_t size) {
      std::string d(size, 'd');
      data = std::move(d);
    }
  };

  class OmapKeySeq
  {
  public:
    std::string s1 = "08b911c5-a313-4c06-a46d-451d064c6570.4100.";
    uint64_t ctr;
    std::string s2 =
      "__multipart_my-multipart-key-1.2~l423STlG8bMdwMMCIW-AWzwCZ8wlX92.meta";
    uint32_t uniq;

    OmapKeySeq(uint32_t uniq) : ctr(), uniq(uniq) {}

    std::string next_key() {
      std::string key;
      key.reserve(s1.length() + s2.length() + 64);
      key.append(s1);
      key.append(std::to_string(++ctr));
      key.append(s2);
      key.append(".");
      key.append(std::to_string(uniq));
      return key;
    }
  };

  class InsertRGWKeys
  {
  public:
    RadosCTX& rctx;
    librados::IoCtx io_ctx;
    std::string obj_name;
    uint32_t uniq;

    InsertRGWKeys(RadosCTX& _rctx, std::string _obj_name, uint32_t _uniq)
      : rctx(_rctx), obj_name(_obj_name), uniq(_uniq) {
      int ret = rctx.rados.ioctx_create(pool.c_str(), io_ctx);
      if (ret < 0) {
	std::cout << "rados_ioctx_create failed " << ret << std::endl;
      }
      ceph::buffer::list bl;
      bl.push_back(
	ceph::buffer::create_static(7, const_cast<char*>("<nihil>")));
      ret = io_ctx.write_full(obj_name, bl);
    }

    void operator()()
    {
      OmapKeySeq seq(uniq);
      OmapVal val(val_size);
      ceph::buffer::list bl;
      bl.append(val.data);
      for(int i = 0; i < n_keys; i++) {
	std::string key = seq.next_key();
	std::map<std::string, ceph::buffer::list> kmap;
	kmap.insert(
	  std::map<std::string, ceph::buffer::list>::value_type(key, bl));
	int ret = io_ctx.omap_set(obj_name, kmap);
	if (ret >= 0) {
	  if (verbose) {
	    std::cout << "inserted: key " << key <<std::endl;
	  }
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
    std::string obj_name;
    uint64_t nread;

    ReadRGWKeys(RadosCTX& _rctx, std::string _obj_name)
      : rctx(_rctx), obj_name(_obj_name), nread(0) {
      int ret = rctx.rados.ioctx_create(pool.c_str(), io_ctx);
      if (ret < 0) {
	std::cout << "rados_ioctx_create failed " << ret << std::endl;
      }
      ceph::buffer::list bl;
      bl.push_back(
	ceph::buffer::create_static(7, const_cast<char*>("<nihil>")));
      ret = io_ctx.write_full(obj_name, bl);
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
	int ret = io_ctx.omap_get_keys2(obj_name, rmarker, max, &keys, &more);
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
    std::string obj_name;

    ClearRGWKeys(RadosCTX& _rctx, std::string _obj_name)
      : rctx(_rctx), obj_name(_obj_name) {
      int ret = rctx.rados.ioctx_create(pool.c_str(), io_ctx);
      if (ret < 0) {
	std::cout << "rados_ioctx_create failed " << ret << std::endl;
      }
    }

    void operator()()
    {
      int ret = io_ctx.remove(obj_name);
      if (ret < 0) {
	std::cout << "rados.remove failed " << ret << std::endl;
      }
    }

    ~ClearRGWKeys() {}

  }; /* ClearRGWKeys */

} /* namespace */

void adhoc_driver(Adhoc op) {
  RadosCTX rctx;

  switch (op) {
  case Adhoc::OP_GET:
    thrds.push_back(std::thread(ReadRGWKeys(rctx, default_object)));
    break;
  case Adhoc::OP_SET:
    for (int ix = 0; ix < n_threads; ++ix) {
      thrds.push_back(std::thread(InsertRGWKeys(rctx, default_object, ix+1))); 
    }
    break;
  case Adhoc::OP_CLEAR:
    thrds.push_back(std::thread(ClearRGWKeys(rctx, default_object)));
    break;
  default:
    break;
  };

  for (auto& thrd : thrds) {
    thrd.join();
  }
}

void player1_driver() {

  /* workload that creates an object, adds nkeys keys w/val of
   * val_size, then deletes the object--repeats indefinitely */

  RadosCTX rctx;
  uint64_t o_suffix = 0;

  for (int ix = 0;; ++ix) {
    std::string obj_name{default_object};
    obj_name += "_";
    obj_name += std::to_string(ix);

    std::cout << __func__
	      << " create " << n_keys << " keys on " << obj_name
	      << std::endl;

    // create keys on obj_name in 1 thread
    thrds.push_back(std::thread(InsertRGWKeys(rctx, obj_name, 1)));
    for (auto& thrd : thrds) {
      thrd.join();
    }
    thrds.clear();

    std::cout << __func__
	      << " remove " << obj_name
	      << std::endl;

    // remove obj_name
    thrds.push_back(std::thread(ClearRGWKeys(rctx, obj_name)));
    for (auto& thrd : thrds) {
      thrd.join();
    }
    thrds.clear();
  } // ix
}

void kali_driver() {

  /* workload that creates and stores nkeys key-val pairs on a
   * sequence of n_objects, then removes each object thus created.
   * repeats indefinitely. */

  RadosCTX rctx;
  uint64_t o_suffix = 0;

  for (int ix = 0;; ++ix) {

    // create cycle
    for (int c_ix = 0; c_ix < n_objects; ++c_ix) {
      std::string obj_name{default_object};
      obj_name += "_";
      obj_name += std::to_string(c_ix);

      std::cout << __func__
		<< " create " << n_keys << " keys on " << obj_name
		<< std::endl;

      // create keys on obj_name in 1 thread
      thrds.push_back(std::thread(InsertRGWKeys(rctx, obj_name, 1)));
      for (auto& thrd : thrds) {
	thrd.join();
      }
      thrds.clear();
    } // c_ix

    // remove cycle
    for (int rm_ix = 0; rm_ix < n_objects; ++rm_ix) {
      std::string obj_name{default_object};
      obj_name += "_";
      obj_name += std::to_string(rm_ix);

      std::cout << __func__
		<< " remove " << obj_name
		<< std::endl;

      // remove obj_name
      thrds.push_back(std::thread(ClearRGWKeys(rctx, obj_name)));
      for (auto& thrd : thrds) {
	thrd.join();
      }
      thrds.clear();
    } // rm_ix
  } // ix
}

void usage(char* prog) {
  std::cout << "usage: \n"
	    << prog << " --get|--set|--clear|--player1|--kali [--verbose]"
	    << " [keys=<n>] [threads=<n>]"
	    << std::endl;
}

int main(int argc, char *argv[])
{
  namespace po = boost::program_options;

  po::options_description desc("Allowed options");
  desc.add_options()
    ("get", "get existing keys")
    ("clear", "clear keys")
    ("set", "set keys")
    ("player1", "non-terminating workload intended to force compactions")
    ("kali", "non-terminating workload intended to force compactions")
    ("verbose", "verbosity")
    ("threads", po::value<int>(), "number of --set threads (default 1)")
    ("keys", po::value<int>(), "number of keys to --set (default 100)")
    ("objects", po::value<int>(), "number of objects to create (kali, def 100)")
    ("valsize", po::value<int>(), "size of omap values to --set (def 200)")
    ("conf", po::value<std::string>(), "path to ceph.conf")
    ("pool", po::value<std::string>(), "RADOS pool")
    ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("conf")) {
    ceph_conf = vm["conf"].as<std::string>();
  }

  if (vm.count("pool")) {
    pool = vm["pool"].as<std::string>();
  }

  if (vm.count("verbose")) {
    verbose = true;
  }

  if (vm.count("threads")) {
    n_threads = vm["threads"].as<int>();
  }

  if (vm.count("keys")) {
    n_keys = vm["keys"].as<int>();
  }

  if (vm.count("objects")) {
    n_objects = vm["objects"].as<int>();
  }

  if (vm.count("valsize")) {
    val_size = vm["valsize"].as<int>();
  }

  if (vm.count("get")) {
    adhoc_driver(Adhoc::OP_GET);
    goto out;
  } else if (vm.count("clear")) {
    adhoc_driver(Adhoc::OP_CLEAR);
    goto out;
  } else if (vm.count("set")) {
    adhoc_driver(Adhoc::OP_SET);
    goto out;
  }

  if (vm.count("player1")) {
    player1_driver();
    goto out;
  }

  if (vm.count("kali")) {
    kali_driver();
    goto out;
  }

  usage(argv[0]);

out:
  return 0;
}
