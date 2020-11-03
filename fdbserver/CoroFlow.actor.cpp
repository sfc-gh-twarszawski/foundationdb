/*
 * CoroFlow.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/CoroFlow.h"
#include "flow/ActorCollection.h"
#include "fdbrpc/libcoroutine/Coro.h"
#include "flow/TDMetric.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h" // has to be last include


Coro *current_coro = 0, *main_coro = 0;
Coro* swapCoro( Coro* n ) {
	Coro* t = current_coro;
	current_coro = n;
	return t;
}

/*struct IThreadlike {
public:
	virtual void start() = 0;     // Call at most once!  Causes run() to be called on the 'thread'.
	virtual ~IThreadlike() {}     // Pre: start hasn't been called, or run() has returned
	virtual void unblock() = 0;   // Pre: block() has been called by run().  Causes block() to return.

protected:
	virtual void block() = 0;     // Call only from run().  Returns when unblock() is called elsewhere.
	virtual void run() = 0;       // To be overridden by client.  Returning causes the thread to block until it is destroyed.
};*/


struct Coroutine /*: IThreadlike*/ {
	Coroutine() {
		coro = Coro_new();
		if (coro == NULL)
			platform::outOfMemory();
	}

	~Coroutine() {
		Coro_free(coro);
	}

	void start() {
		int result = Coro_startCoro_( swapCoro(coro), coro, this, &entry );
		if (result == ENOMEM) platform::outOfMemory();
	}

	void unblock() {
		//Coro_switchTo_( swapCoro(coro), coro );
		blocked.send(Void());
	}

protected:
	void block() {
		//Coro_switchTo_( swapCoro(main_coro), main_coro );
		blocked = Promise<Void>();
		double before = now();
		CoroThreadPool::waitFor( blocked.getFuture() );
		if (g_network->isSimulated() && g_simulator.getCurrentProcess()->rebooting) TraceEvent("CoroUnblocked").detail("After", now()-before);
	}

	virtual void run() = 0;

private:
	void wrapRun() {
		run();
		Coro_switchTo_( swapCoro(main_coro), main_coro );
		//block();
	}

	static void entry(void* _this) {
		((Coroutine*)_this)->wrapRun();
	}

	Coro* coro;
	Promise<Void> blocked;
};

template <class Threadlike, class Mutex, bool IS_CORO>
class WorkPool : public IThreadPool, public ReferenceCounted<WorkPool<Threadlike,Mutex,IS_CORO>> {
	struct Worker;

	// Pool can survive the destruction of WorkPool while it waits for workers to terminate
	struct Pool : ReferenceCounted<Pool> {
		Mutex queueLock;
		Deque<PThreadAction> work;
		std::vector<Worker*> idle, workers;
		ActorCollection anyError, allStopped;
		Future<Void> m_holdRefUntilStopped;

		Pool() : anyError(false), allStopped(true) {
			m_holdRefUntilStopped = holdRefUntilStopped(this);
		}

		~Pool() {
			for(int c=0; c<workers.size(); c++)
				delete workers[c];
		}

		ACTOR Future<Void> holdRefUntilStopped( Pool* p ) {
			p->addref();
			wait( p->allStopped.getResult() );
			p->delref();
			return Void();
		}
	};

	struct Worker : Threadlike {
		Pool* pool;
		IThreadPoolReceiver* userData;
		bool stop;
		ThreadReturnPromise<Void> stopped;
		ThreadReturnPromise<Void> error;

		Worker( Pool* pool,  IThreadPoolReceiver* userData ) : pool(pool), userData(userData), stop(false) {
		}

		virtual void run() {
			try {
				if(!stop)
					userData->init();

				while (!stop) {
					pool->queueLock.enter();
					if (pool->work.empty()) {
						pool->idle.push_back( this );
						pool->queueLock.leave();
						Threadlike::block();
					} else {
						PThreadAction a = pool->work.front();
						pool->work.pop_front();
						pool->queueLock.leave();
						(*a)(userData);
						if(IS_CORO) CoroThreadPool::waitFor(yield());
					}
				}

				TraceEvent("CoroStop");
				delete userData;
				stopped.send(Void());
				return;
			} catch (Error& e) {
				TraceEvent("WorkPoolError").error(e, true);
				error.sendError(e);
			} catch (...) {
				TraceEvent("WorkPoolError");
				error.sendError(unknown_error());
			}

			try {
				delete userData;
			} catch (...) {
				TraceEvent(SevError, "WorkPoolErrorShutdownError");
			}
			stopped.send(Void());
		}
	};

	Reference<Pool> pool;
	Future<Void> m_stopOnError;  // must be last, because its cancellation calls stop()!
	Error error;

	ACTOR Future<Void> stopOnError( WorkPool* w ) {
		try {
			wait(w->getError());
			ASSERT(false);
		} catch (Error& e) {
			w->stop(e);
		}
		return Void();
	}

	void checkError() {
		if (error.code() != invalid_error_code) {
			ASSERT( error.code() != error_code_success );  // Calling post or addThread after stop is an error
			throw error;
		}
	}

public:
	WorkPool() : pool( new Pool ) {
		m_stopOnError = stopOnError( this );
	}

	virtual Future<Void> getError() { return pool->anyError.getResult(); }
	virtual void addThread(IThreadPoolReceiver* userData, const char*) {
		checkError();

		auto w = new Worker(pool.getPtr(), userData);
		pool->queueLock.enter();
		pool->workers.push_back( w );
		pool->queueLock.leave();
		pool->anyError.add( w->error.getFuture() );
		pool->allStopped.add( w->stopped.getFuture() );
		startWorker(w);
	}
	ACTOR static void startWorker( Worker* w ) {
		// We want to make sure that coroutines are always started after Net2::run() is called, so the main coroutine is
		// initialized.
		wait( delay(0, g_network->getCurrentTask() ));
		w->start();
	}
	virtual void post( PThreadAction action ) {
		checkError();

		pool->queueLock.enter();
		pool->work.push_back(action);
		if (!pool->idle.empty()) {
			Worker* c = pool->idle.back();
			pool->idle.pop_back();
			pool->queueLock.leave();
			c->unblock();
		} else
			pool->queueLock.leave();
	}
	virtual Future<Void> stop(Error const& e) {
		if (error.code() == invalid_error_code) {
			error = e;
		}

		pool->queueLock.enter();
		TraceEvent("WorkPool_Stop").detail("Workers", pool->workers.size()).detail("Idle", pool->idle.size())
			.detail("Work", pool->work.size()).error(e, true);

		for (uint32_t i=0; i<pool->work.size(); i++)
			pool->work[i]->cancel();   // What if cancel() does something to this?
		pool->work.clear();
		for(int i=0; i<pool->workers.size(); i++)
			pool->workers[i]->stop = true;

		std::vector<Worker*> idle;
		std::swap(idle, pool->idle);
		pool->queueLock.leave();

		for(int i=0; i<idle.size(); i++)
			idle[i]->unblock();

		pool->allStopped.add( Void() );

		return pool->allStopped.getResult();
	}
	virtual bool isCoro() const { return IS_CORO; }
	virtual void addref() { ReferenceCounted<WorkPool>::addref(); }
	virtual void delref() { ReferenceCounted<WorkPool>::delref(); }
};

typedef WorkPool<Coroutine, ThreadUnsafeSpinLock, true> CoroPool;



ACTOR void coroSwitcher( Future<Void> what, TaskPriority taskID, Coro* coro ) {
	try {
		// state double t = now();
		wait(what);
		//if (g_network->isSimulated() && g_simulator.getCurrentProcess()->rebooting && now()!=t)
		//	TraceEvent("NonzeroWaitDuringReboot").detail("TaskID", taskID).detail("Elapsed", now()-t).backtrace("Flow");
	} catch (Error&) {}
	wait( delay(0, taskID) );
	Coro_switchTo_( swapCoro(coro), coro );
}



void CoroThreadPool::waitFor( Future<Void> what ) {
	ASSERT (current_coro != main_coro);
	if (what.isReady()) return;
	// double t = now();
	coroSwitcher(what, g_network->getCurrentTask(), current_coro);
	Coro_switchTo_( swapCoro(main_coro), main_coro );
	//if (g_network->isSimulated() && g_simulator.getCurrentProcess()->rebooting && now()!=t)
	//	TraceEvent("NonzeroWaitDuringReboot").detail("TaskID", currentTaskID).detail("Elapsed", now()-t).backtrace("Coro");
	ASSERT( what.isReady() );
}

// Right After INet2::run
void CoroThreadPool::init()
{
	if (!current_coro) {
		current_coro = main_coro = Coro_new();
		if (main_coro == NULL) platform::outOfMemory();

		Coro_initializeMainCoro(main_coro);
		//printf("Main thread: %d bytes stack presumed available\n", Coro_bytesLeftOnStack(current_coro));
	}
}


Reference<IThreadPool> CoroThreadPool::createThreadPool() {
	return Reference<IThreadPool>( new CoroPool );
}
