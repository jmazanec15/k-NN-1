#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#

echo "Starting script"
pwd
gcc --version | head -n 1 | cut -d ' ' -f3
echo "Running the scl_setup"
bash /usr/local/bin/scl_setup
git submodule update --init -- jni/external/nmslib
git submodule update --init -- jni/external/faiss

cd jni/external/faiss
git branch
git status
git apply --ignore-space-change --ignore-whitespace --3way ../../patches/faiss/0001-Custom-patch-to-support-multi-vector.patch
rm ../../patches/faiss/0001-Custom-patch-to-support-multi-vector.patch
git apply --ignore-space-change --ignore-whitespace --3way ../../patches/faiss/0002-Enable-precomp-table-to-be-shared-ivfpq.patch
rm ../../patches/faiss/0002-Enable-precomp-table-to-be-shared-ivfpq.patch


cd ../nmslib
git apply --ignore-space-change --ignore-whitespace --3way ../../patches/nmslib/0001-Initialize-maxlevel-during-add-from-enterpoint-level.patch
rm ../../patches/nmslib/0001-Initialize-maxlevel-during-add-from-enterpoint-level.patch
cd ../../
cmake . -DSIMD_ENABLED=false
make
cd ..
pwd
ls