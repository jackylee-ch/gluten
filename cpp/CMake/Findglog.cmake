# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set(GLUTEN_GLOG_MINIMUM_VERSION 0.4.0)
set(GLUTEN_GLOG_VERSION 0.6.0)

if(NOT BUILD_GLOG)
  include(FindPackageHandleStandardArgs)
  include(SelectLibraryConfigurations)

  # On macOS, prefer the static libglog.a over libglog.dylib. libvelox.dylib has
  # static gflags baked in via folly, so we must avoid libglog.dylib which
  # transitively loads libgflags.dylib at runtime and triggers the "linked both
  # statically and dynamically" abort in gflags.
  if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set(_glog_find_suffixes_saved ${CMAKE_FIND_LIBRARY_SUFFIXES})
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a" ${CMAKE_FIND_LIBRARY_SUFFIXES})
  endif()

  find_library(GLOG_LIBRARY_RELEASE glog PATHS ${GLOG_LIBRARYDIR})
  find_library(GLOG_LIBRARY_DEBUG glogd PATHS ${GLOG_LIBRARYDIR})

  if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set(CMAKE_FIND_LIBRARY_SUFFIXES ${_glog_find_suffixes_saved})
    unset(_glog_find_suffixes_saved)
  endif()

  find_path(GLOG_INCLUDE_DIR glog/logging.h PATHS ${GLOG_INCLUDEDIR})

  select_library_configurations(GLOG)

  find_package_handle_standard_args(glog DEFAULT_MSG GLOG_LIBRARY
                                    GLOG_INCLUDE_DIR)

  mark_as_advanced(GLOG_LIBRARY GLOG_INCLUDE_DIR)
endif()

if(NOT glog_FOUND)
  include(BuildGlog)
endif()

get_filename_component(libglog_ext ${GLOG_LIBRARY} EXT)
if(libglog_ext STREQUAL ".a")
  set(libglog_type STATIC)
  set(libgflags_component static)
else()
  set(libglog_type SHARED)
  set(libgflags_component shared)
endif()

# On macOS, force static gflags regardless of glog linkage. libvelox.dylib has
# static gflags baked in via folly (-DGFLAGS_SHARED=FALSE). If libgluten.dylib
# were to load libgflags.dylib dynamically, gflags would detect the same flag
# registered both statically (inside libvelox.dylib) and dynamically (via
# libgflags.dylib) and abort with "being linked both statically and
# dynamically".
if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  set(libgflags_component static)
endif()

# On macOS with a static libglog.a we link glog and gflags via -load_hidden so
# their symbols stay local to libgluten.dylib. This prevents dyld from
# coalescing the weak symbols (C++ function-local statics like
# FlagRegistry::GlobalRegistry) with the static gflags copy already inside
# libvelox.dylib, which otherwise triggers "flag 'flagfile' ... linked both
# statically and dynamically".
#
# For this to work, google::glog must be an INTERFACE IMPORTED target so that
# CMake does not try to add GLOG_LIBRARY through the normal link path.
set(_gluten_glog_hidden_load OFF)
if(CMAKE_SYSTEM_NAME MATCHES "Darwin" AND libglog_ext STREQUAL ".a")
  set(_gluten_glog_hidden_load ON)
endif()

# glog::glog may already exist. Use google::glog to avoid conflicts.
if(_gluten_glog_hidden_load)
  add_library(google::glog INTERFACE IMPORTED)
  set_target_properties(google::glog PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                "${GLOG_INCLUDE_DIR}")
else()
  add_library(google::glog ${libglog_type} IMPORTED)
  set_target_properties(google::glog PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                                "${GLOG_INCLUDE_DIR}")
  set_target_properties(
    google::glog PROPERTIES IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                            IMPORTED_LOCATION "${GLOG_LIBRARY}")
endif()

set(GLUTEN_GFLAGS_VERSION 2.2.2)
find_package(gflags ${GLUTEN_GFLAGS_VERSION} CONFIG
             COMPONENTS ${libgflags_component})

if(NOT gflags_FOUND AND glog_FOUND)
  message(
    FATAL_ERROR
      "Glog found but Gflags not found. Set BUILD_GLOG=ON and reload cmake.")
endif()

if(gflags_FOUND)
  if(NOT TARGET gflags::gflags_${libgflags_component}
     AND NOT TARGET gflags_${libgflags_component})
    message(
      FATAL_ERROR
        "Found Gflags but missing component gflags_${libgflags_component}")
  endif()
  if(NOT _gluten_glog_hidden_load)
    if(TARGET gflags::gflags_${libgflags_component})
      set_target_properties(
        google::glog PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES
                                gflags::gflags_${libgflags_component})
    else()
      set_target_properties(
        google::glog PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES
                                gflags_${libgflags_component})
    endif()
  endif()
else()
  include(BuildGflags)
  if(NOT _gluten_glog_hidden_load)
    set_target_properties(
      google::glog PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES gflags_static)
  endif()
endif()

if(_gluten_glog_hidden_load)
  set(_gflags_static_target "")
  if(TARGET gflags::gflags_static)
    set(_gflags_static_target gflags::gflags_static)
  elseif(TARGET gflags_static)
    set(_gflags_static_target gflags_static)
  endif()

  set(_gflags_static_path "")
  if(_gflags_static_target)
    foreach(_cfg RELEASE NOCONFIG "" DEBUG RELWITHDEBINFO MINSIZEREL)
      if(_cfg STREQUAL "")
        set(_prop IMPORTED_LOCATION)
      else()
        set(_prop IMPORTED_LOCATION_${_cfg})
      endif()
      get_target_property(_maybe_path ${_gflags_static_target} ${_prop})
      if(_maybe_path AND NOT _maybe_path STREQUAL "_maybe_path-NOTFOUND")
        set(_gflags_static_path ${_maybe_path})
        break()
      endif()
    endforeach()
  endif()

  if(NOT _gflags_static_path)
    message(
      FATAL_ERROR
        "Could not resolve libgflags.a path for -load_hidden; expected a gflags_static target with an IMPORTED_LOCATION* property."
    )
  endif()

  message(
    STATUS "Linking gflags (static, -load_hidden): ${_gflags_static_path}")

  set_target_properties(
    google::glog PROPERTIES INTERFACE_LINK_OPTIONS
                            "LINKER:-load_hidden,${GLOG_LIBRARY}")
  set_property(
    TARGET google::glog
    APPEND
    PROPERTY INTERFACE_LINK_OPTIONS
             "LINKER:-load_hidden,${_gflags_static_path}")
endif()
