/*
 * BlobBuilder.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/WorkerInterface.actor.h"
#include <iostream>
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/serialize.h"

ACTOR Future<Void> blobbuilder(Database* db) {
	TraceEvent("BlobBuilderStarting");
	loop {
		state ReadYourWritesTransaction tr(*db);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

				Optional<Value> bloblKeyRangeVal = wait(tr.get(blobBuilderRangeKey));

				if (bloblKeyRangeVal.present()) {
					KeyRangeRef blobKeyRange;
					BinaryReader br(bloblKeyRangeVal.get(), IncludeVersion());
					br.deserialize<KeyRangeRef>(blobKeyRange);
					TraceEvent("BlobBuilderRangeUpdate").detail("Range", blobKeyRange);
				} else {
					TraceEvent("BlobBuilderRangeUpdate").detail("Range", "empty");
				}

				state Future<Void> blobBuilderRangeChangeFuture = tr.watch(blobBuilderRangeKey);

				wait(tr.commit());
				wait(blobBuilderRangeChangeFuture);

				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}