#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Verify the bundled gluten-velox jar's Arrow C-Data classes reference the
# *unshaded* Apache Arrow API — both in their method signatures and in their
# constant pools.
#
# Background: org.apache.arrow.c.* must NOT be relocated (its native JNI binds
# to the original class names), but it reaches into three other Arrow packages:
# org.apache.arrow.memory.*, org.apache.arrow.vector.* (public signatures) and
# org.apache.arrow.util.* (internal calls — Preconditions, AutoCloseables,
# Collections2). All three must stay unshaded in the bundle:
#
#   - a shaded *signature* type re-binds the bundled ArrowArrayStream/ArrowSchema
#     so any caller passing a vanilla Apache Arrow allocator hits
#     `NoSuchMethodError` (gluten#12225);
#   - a shaded *constant-pool* reference is worse when Arrow is no longer
#     bundled at all: the shaded target does not exist anywhere on the
#     classpath and the call site throws `ClassNotFoundException`.
#
# Usage:
#   dev/check-arrow-c-shading.sh <path-to-gluten-velox-bundle.jar> [shade-package-name]
#
# The shade package name defaults to org.apache.gluten.shaded and is passed by
# package/pom.xml as ${gluten.shade.packageName}. Keep it parameterized: if this
# script hard-coded the prefix and the Maven property were ever changed, both
# checks below would silently match nothing and the whole guard would pass
# vacuously.
#
# Exit codes:
#   0 — bundle is well-shaded (Arrow C-Data API uses public Apache Arrow API)
#   1 — bundle is broken (Arrow C-Data references gluten-shaded types)
#   2 — usage / setup error

set -euo pipefail

JAR="${1:?usage: $0 <path-to-gluten-velox-bundle.jar> [shade-package-name]}"
if [[ ! -f "$JAR" ]]; then
  echo "error: jar not found: $JAR" >&2
  exit 2
fi

# Dotted form for javap signatures, slashed form for JVM internal names in
# constant pools. `.` is escaped so the dotted form is a literal regex.
SHADE_PACKAGE="${2:-org.apache.gluten.shaded}"
SHADE_DOTS_RE="${SHADE_PACKAGE//./\\.}"
SHADE_SLASHES="${SHADE_PACKAGE//.//}"

if ! command -v javap >/dev/null; then
  echo "error: javap not found on PATH" >&2
  exit 2
fi

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

# Classes whose public API touches the unshaded boundary.
CLASSES=(
  "org/apache/arrow/c/ArrowArrayStream"
  "org/apache/arrow/c/ArrowSchema"
  "org/apache/arrow/c/ArrowArray"
  "org/apache/arrow/c/Data"
)

failures=0
for cls in "${CLASSES[@]}"; do
  if ! unzip -p "$JAR" "${cls}.class" > "$WORKDIR/$(basename "$cls").class" 2>/dev/null; then
    echo "  SKIP $cls (not in bundle)"
    continue
  fi
  signatures=$(javap -p "$WORKDIR/$(basename "$cls").class" 2>/dev/null || true)
  # Any method signature mentioning the shaded Arrow path is the bug.
  bad=$(echo "$signatures" | grep -E "${SHADE_DOTS_RE}\.org\.apache\.arrow\.(memory|vector)\." || true)
  if [[ -n "$bad" ]]; then
    echo "  FAIL $cls — public API references gluten-shaded Arrow types:"
    echo "$bad" | sed 's/^/    /'
    failures=$((failures + 1))
  else
    echo "  OK   $cls"
  fi
done

# Second check: no org.apache.arrow.c.* class may *call* a shaded Arrow class.
# Signatures alone miss org.apache.arrow.util.Preconditions & friends, which are
# invoked from constructors but never appear in a descriptor.
#
# The name pattern covers every character legal in a JVM internal name after the
# package prefix: identifier chars (letters, digits, `_`, `$`), `/` for nested
# packages, and `-` for the synthetic `package-info` / `module-info` entries.
mkdir -p "$WORKDIR/all"
unzip -qo "$JAR" 'org/apache/arrow/c/*' -d "$WORKDIR/all" 2>/dev/null || true
if compgen -G "$WORKDIR/all/org/apache/arrow/c/*.class" > /dev/null; then
  refs=$(grep -rahoE "${SHADE_SLASHES}/org/apache/arrow/[a-zA-Z0-9_$/-]+" \
    "$WORKDIR/all/org/apache/arrow/c" 2>/dev/null | sort -u || true)
  if [[ -n "$refs" ]]; then
    echo "  FAIL org/apache/arrow/c/** — calls into gluten-shaded Arrow:"
    echo "$refs" | sed 's/^/    /'
    failures=$((failures + 1))
  else
    echo "  OK   org/apache/arrow/c/** constant pools"
  fi
fi

if (( failures > 0 )); then
  echo
  echo "Bundle has $failures Arrow C-Data shading problem(s)."
  echo "See gluten#12225 for context. Update package/pom.xml's"
  echo "<relocation org.apache.arrow> excludes so every package reachable"
  echo "from org.apache.arrow.c stays unshaded (memory, vector, util)."
  exit 1
fi

echo
echo "All Arrow C-Data classes use unshaded public Apache Arrow API. ✓"
