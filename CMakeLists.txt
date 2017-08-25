
cmake_minimum_required (VERSION 2.8.11)
project(omap_tf_compact CXX)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

include(CheckIncludeFiles)
include(CheckLibraryExists)
include(CheckCXXCompilerFlag)
include(FindPkgConfig)

find_package(RADOS)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g3 -O0 -std=c++11")

add_executable (omap_tf_compact
  omap_tf_compact.cc)

if (RADOS_PREFIX)
  target_include_directories(omap_tf_compact PUBLIC
    "${RADOS_PREFIX}/include")
endif()

target_link_libraries(omap_tf_compact LINK_PUBLIC
  ${RADOS_LIBRARIES}
  boost_program_options
  boost_date_time
  pthread
  )
