/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <substrait/algebra.pb.h>

namespace local_engine
{
using substraitInputFile = substrait::ReadRel::LocalFiles::FileOrFiles;
using substraitIcebergDeleteFile = substrait::ReadRel::LocalFiles::FileOrFiles::IcebergReadOptions::DeleteFile;
using IcebergReadOptions = substrait::ReadRel::LocalFiles::FileOrFiles::IcebergReadOptions;
using FileContent = IcebergReadOptions::FileContent;

}