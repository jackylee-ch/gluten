// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// macOS only: libgluten.dylib loads libgflags.a with -load_hidden
// (cpp/CMake/Findglog.cmake) so its gflags symbols do not coalesce with the
// static gflags already baked into libvelox.dylib. libvelox.dylib's own
// static gflags build pulled gflags.cc.o but not gflags_reporting.cc.o, so
// google::HandleCommandLineHelpFlags remains undefined inside libvelox.dylib
// and is expected to be supplied by libgluten.dylib at dlopen time. Provide a
// no-op stub with default visibility; Gluten never invokes help processing.
namespace google {
// NOLINTNEXTLINE(readability-identifier-naming) - name dictated by gflags ABI.
__attribute__((visibility("default"))) void HandleCommandLineHelpFlags() {}
} // namespace google
