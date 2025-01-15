/*-------------------------------------------------------------------------
 *
 * freelist.c
 *	  routines for managing the buffer pool's replacement strategy.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/freelist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pgstat.h"
#include "port/atomics.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"

#define INT_ACCESS_ONCE(var)	((int)(*((volatile int *)&(var))))

/*
 * The shared freelist control information.
 */
typedef struct
{
	/* Spinlock: protects the values below */
	slock_t		buffer_strategy_lock;

	/*
	 * Clock sweep hand: index of next buffer to consider grabbing. Note that
	 * this isn't a concrete buffer - we only ever increase the value. So, to
	 * get an actual buffer, it needs to be used modulo NBuffers.
	 */
	pg_atomic_uint32 nextVictimBuffer;

	int			firstFreeBuffer;	/* Head of list of unused buffers */
	int			lastFreeBuffer; /* Tail of list of unused buffers */

	/*
	 * NOTE: lastFreeBuffer is undefined when firstFreeBuffer is -1 (that is,
	 * when the list is empty)
	 */

	/*
	 * Statistics.  These counters should be wide enough that they can't
	 * overflow during a single bgwriter cycle.
	 */
	uint32		completePasses; /* Complete cycles of the clock sweep */
	pg_atomic_uint32 numBufferAllocs;	/* Buffers allocated since last reset */

	/*
	 * Bgworker process to be notified upon activity or -1 if none. See
	 * StrategyNotifyBgWriter.
	 */
	int			bgwprocno;
} BufferStrategyControl;

typedef struct 
{
	int tag;
	int bufferDescIndex;
} RingBufferEntry;

typedef struct 
{
	slock_t ring_buffer_lock;
	int head;
	int tail;
	int currentSize;
	int maxSize;
	RingBufferEntry queue[FLEXIBLE_ARRAY_MEMBER];
} RingBuffer;

typedef struct {
	slock_t lock;
	bool isIdxFree[FLEXIBLE_ARRAY_MEMBER];
} OrphanBufferArray;

typedef struct {
	int refCount;
	int usageCount;
} ReferenceUsagePair;

/* Pointers to shared state */
static BufferStrategyControl *StrategyControl = NULL;

static RingBuffer *S3MainQueue = NULL;
static RingBuffer *S3ProbationaryQueue = NULL;
static RingBuffer *S3GhostQueue = NULL;

static OrphanBufferArray *S3OrphanBufferIndexes = NULL;

/*
 * Private (non-shared) state for managing a ring of shared buffers to re-use.
 * This is currently the only kind of BufferAccessStrategy object, but someday
 * we might have more kinds.
 */
typedef struct BufferAccessStrategyData
{
	/* Overall strategy type */
	BufferAccessStrategyType btype;
	/* Number of elements in buffers[] array */
	int			nbuffers;

	/*
	 * Index of the "current" slot in the ring, ie, the one most recently
	 * returned by GetBufferFromRing.
	 */
	int			current;

	/*
	 * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
	 * have not yet selected a buffer for this ring slot.  For allocation
	 * simplicity this is palloc'd together with the fixed fields of the
	 * struct.
	 */
	Buffer		buffers[FLEXIBLE_ARRAY_MEMBER];
}			BufferAccessStrategyData;


/* Prototypes for internal functions */
static BufferDesc *GetBufferFromRing(BufferAccessStrategy strategy,
									 uint32 *buf_state);
static void AddBufferToRing(BufferAccessStrategy strategy,
							BufferDesc *buf);

/*
 * Checks if ring buffer is full
*/
static inline bool IsRingBufferFull(RingBuffer* rb) {
	return rb->currentSize == rb->maxSize;
}

/*
 * Checks if ring buffer is empty
*/
static inline bool IsRingBufferEmpty(RingBuffer* rb) {
	return rb->currentSize == 0;
}

/*
 * Adds a new element to the ring buffer head
 * If an item has to be evicted to make space, returns that item
 * Else returns {-1, -1}
*/
static RingBufferEntry RingBufferEnqueue(RingBuffer* rb, int tag, int idx) {
	RingBufferEntry victim = {.tag = -1, .bufferDescIndex = -1};

	if (IsRingBufferFull(rb)) {
		victim.tag = rb->queue[rb->tail].tag;
		victim.bufferDescIndex = rb->queue[rb->tail].bufferDescIndex;
		rb->tail = (rb->tail + 1) % rb->maxSize;
	} else {
		rb->currentSize++;
	}

	rb->queue[rb->head].tag = tag;
	rb->queue[rb->head].bufferDescIndex = idx;
	rb->head = (rb->head + 1) % rb->maxSize;

	return victim;
}

typedef enum {
	RB_BY_TAG,
	RB_BY_INDEX
} RingBufferOperationMode;

/*
 * Search ringbuffer for buffer index or tag (denoted by parameter mode)
 * Returns index of result in rb->queue or -1 if not found
*/
static int SearchRingBuffer(RingBuffer* rb, int target, RingBufferOperationMode mode) {
	if (IsRingBufferEmpty(rb)) return -1;

	int count = rb->currentSize;
	int idx = rb->tail;
	while (count > 0) {
		if ((mode == RB_BY_TAG && rb->queue[idx].tag == target) ||
			(mode == RB_BY_INDEX && rb->queue[idx].bufferDescIndex == target)) {
			return idx;
		}
		idx = (idx + 1) % rb->maxSize;
		count--;
	}
	return idx;
}

/*
 * Deletes entry from ring buffer based on buffer index or tag (denoted by parameter mode)
 * Returns true if item was present and deleted, returns false if index not found
*/
static bool DeleteFromRingBuffer(RingBuffer* rb, int target, RingBufferOperationMode mode) {
	if (IsRingBufferEmpty(rb)) return false;

	int idx = SearchRingBuffer(rb, target, mode);
	if (idx == -1) return false;

	int current = idx;
	int next = (idx + 1) % rb->maxSize;
	while (current != rb->head) {
		rb->queue[current] = rb->queue[next];
		current = next;
		next = (next + 1) % rb->maxSize;
	}

	rb->head = (rb->head + rb->maxSize - 1) % rb->maxSize;
	rb->currentSize--;

    // Handle edge case of single element
	if (rb->currentSize == 0) {
		rb->head = 0;
		rb->tail = 0;
	}

	return true;
}

static ReferenceUsagePair GetRefUsageCount(int idx) {
	BufferDesc* newBuf = GetBufferDescriptor(idx);
	uint32 newBufState = LockBufHdr(newBuf);
	ReferenceUsagePair result;
	result.refCount = BUF_STATE_GET_REFCOUNT(newBufState);
	result.usageCount = BUF_STATE_GET_USAGECOUNT(newBufState);
	UnlockBufHdr(newBuf, newBufState);
	return result;
}

static void DecrementUsageCount(int idx) {
	BufferDesc* newBuf = GetBufferDescriptor(idx);
	uint32 newBufState = LockBufHdr(newBuf);
	if (BUF_STATE_GET_USAGECOUNT(newBufState) > 0) newBufState -= BUF_USAGECOUNT_ONE;
	UnlockBufHdr(newBuf, newBufState);
}

static void ZeroUsageCount(int idx) {
	BufferDesc* newBuf = GetBufferDescriptor(idx);
	uint32 newBufState = LockBufHdr(newBuf);
	while (BUF_STATE_GET_USAGECOUNT(newBufState) > 0) {
		newBufState -= BUF_USAGECOUNT_ONE;
	}
	UnlockBufHdr(newBuf, newBufState);
}

/*
 * Performs FIFO-Reinsertion on the given ring buffer, 
 * using the given tag and idx as the first to insert
 * Returns the buffer descriptor index held by the evicted entry
*/
static int FifoReinsertion(RingBuffer* rb, int firstTag, int firstIdx) {	
	int newTag = firstTag;
	int newIdx = firstIdx;
	int bufferSize = rb->maxSize;
	while (bufferSize > 0) {
		RingBufferEntry enqueueResult = RingBufferEnqueue(rb, newTag, newIdx);
		if (newIdx != -1) DecrementUsageCount(newIdx);
		if (enqueueResult.tag == -1 && enqueueResult.bufferDescIndex == -1) break;
		
		ReferenceUsagePair metrics;
		if (enqueueResult.bufferDescIndex != -1) {
			metrics = GetRefUsageCount(enqueueResult.bufferDescIndex);
		} else if (enqueueResult.tag == firstTag) {
			// reinsert if not yet assigned an index (edge case)
			metrics.refCount = 1;
			metrics.usageCount = 1;
		}

		if (metrics.refCount > 0 || metrics.usageCount > 0) {
			newTag = enqueueResult.tag;
			newIdx = enqueueResult.bufferDescIndex;
		} else {
			newIdx = enqueueResult.bufferDescIndex;
			break;
		}
		bufferSize--;
	}

	// If nothing could be evicted from the main queue, dip into the pool of orphaned buffer indexes
	if (newIdx == -1) {
		SpinLockAcquire(&S3OrphanBufferIndexes->lock);
		for (int i = 0; i < NBuffers; i++) {
			if (S3OrphanBufferIndexes->isIdxFree[i]) {
				newIdx = i;
				S3OrphanBufferIndexes->isIdxFree[i] = false;
				break;
			}
		}
		SpinLockRelease(&S3OrphanBufferIndexes->lock);
	}

	if (newIdx == -1) elog(ERROR, "S3-FIFO: No unpinned buffers available");

	return newIdx;
}

/*
 * Throws an error if rb contains any indexes outside of range [0, NBuffers)
*/
static void CheckIdxRange(RingBuffer* rb) {
	if (IsRingBufferEmpty(rb)) ereport(LOG, errmsg("Fine"));
	int count = rb->currentSize;
	int idx = rb->tail;
	while (count > 0) {
		if (rb->queue[idx].bufferDescIndex < 0) {
			elog(ERROR, "S3-FIFO: Negative index detected!!!");
			return;
		} else if (rb->queue[idx].bufferDescIndex > NBuffers - 1) {
			elog(ERROR, "S3-FIFO: Index larger than NBuffers detected!!!");
			return;
		}
		idx = (idx + 1) % rb->maxSize;
		count--;
	}
}

/*
 * Throws an error if rb1 and rb2 share buffer indexes
*/
static void CheckForSharedIdxs(RingBuffer* rb1, RingBuffer* rb2) {
	if (!rb1 || !rb2) {
		elog(ERROR, "S3-FIFO: Null FIFO queue");
	}

	if (rb1->currentSize == 0 || rb2->currentSize == 0) return;

	for (int i = 0; i < rb1->currentSize; i++) {
		int idx1 = (rb1->head + i) % rb1->maxSize;
		RingBufferEntry elem1 = rb1->queue[idx1];

		for (int j = 0; j < rb2->currentSize; j++) {
			int idx2 = (rb2->head + j) % rb2->maxSize;
			RingBufferEntry elem2 = rb2->queue[idx2];

			if (elem1.bufferDescIndex == elem2.bufferDescIndex) {
				elog(ERROR, "S3-FIFO: Shared buffer index");
			}
		}
	}
}

/*
 * Throws an error if rb contains any duplicate elements
*/
static void CheckForDuplicateIdxs(RingBuffer *rb) {
    if (rb == NULL) {
        elog(ERROR, "S3-FIFO: NULL FIFO queue");
    }
	if (rb->currentSize <= 1) {
		return;
	}

    for (int i = 0; i < rb->currentSize; ++i) {
        int idx1 = (rb->head + i) % rb->maxSize;
        for (int j = i + 1; j < rb->currentSize; ++j) {
            int idx2 = (rb->head + j) % rb->maxSize;

            if (rb->queue[idx1].bufferDescIndex == rb->queue[idx2].bufferDescIndex) {
                elog(ERROR, "S3-FIFO: Duplicate idx");
            }
        }
    }
}

/*
 * ClockSweepTick - Helper routine for StrategyGetBuffer()
 *
 * Move the clock hand one buffer ahead of its current position and return the
 * id of the buffer now under the hand.
 */
static inline uint32
ClockSweepTick(void)
{
	uint32		victim;

	/*
	 * Atomically move hand ahead one buffer - if there's several processes
	 * doing this, this can lead to buffers being returned slightly out of
	 * apparent order.
	 */
	victim =
		pg_atomic_fetch_add_u32(&StrategyControl->nextVictimBuffer, 1);

	if (victim >= NBuffers)
	{
		uint32		originalVictim = victim;

		/* always wrap what we look up in BufferDescriptors */
		victim = victim % NBuffers;

		/*
		 * If we're the one that just caused a wraparound, force
		 * completePasses to be incremented while holding the spinlock. We
		 * need the spinlock so StrategySyncStart() can return a consistent
		 * value consisting of nextVictimBuffer and completePasses.
		 */
		if (victim == 0)
		{
			uint32		expected;
			uint32		wrapped;
			bool		success = false;

			expected = originalVictim + 1;

			while (!success)
			{
				/*
				 * Acquire the spinlock while increasing completePasses. That
				 * allows other readers to read nextVictimBuffer and
				 * completePasses in a consistent manner which is required for
				 * StrategySyncStart().  In theory delaying the increment
				 * could lead to an overflow of nextVictimBuffers, but that's
				 * highly unlikely and wouldn't be particularly harmful.
				 */
				SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

				wrapped = expected % NBuffers;

				success = pg_atomic_compare_exchange_u32(&StrategyControl->nextVictimBuffer,
														 &expected, wrapped);
				if (success)
					StrategyControl->completePasses++;
				SpinLockRelease(&StrategyControl->buffer_strategy_lock);
			}
		}
	}
	return victim;
}

/*
 * have_free_buffer -- a lockless check to see if there is a free buffer in
 *					   buffer pool.
 *
 * If the result is true that will become stale once free buffers are moved out
 * by other operations, so the caller who strictly want to use a free buffer
 * should not call this.
 */
bool
have_free_buffer(void)
{
	if (StrategyControl->firstFreeBuffer >= 0)
		return true;
	else
		return false;
}

/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	BufferAlloc(). The only hard requirement BufferAlloc() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 *	To ensure that no one else can pin the buffer before we do, we must
 *	return the buffer with the buffer header spinlock still held.
 */
BufferDesc *
StrategyGetBuffer(BufferAccessStrategy strategy, uint32 *buf_state, bool *from_ring, uint32 tagHash)
{
	BufferDesc *buf;
	int			bgwprocno;
	int			trycounter;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */

	*from_ring = false;

	/*
	 * If given a strategy object, see whether it can select a buffer. We
	 * assume strategy objects don't need buffer_strategy_lock.
	 */
	if (strategy != NULL)
	{
		buf = GetBufferFromRing(strategy, buf_state);
		if (buf != NULL)
		{
			*from_ring = true;
			return buf;
		}
	}

	/*
	 * If asked, we need to waken the bgwriter. Since we don't want to rely on
	 * a spinlock for this we force a read from shared memory once, and then
	 * set the latch based on that value. We need to go through that length
	 * because otherwise bgwprocno might be reset while/after we check because
	 * the compiler might just reread from memory.
	 *
	 * This can possibly set the latch of the wrong process if the bgwriter
	 * dies in the wrong moment. But since PGPROC->procLatch is never
	 * deallocated the worst consequence of that is that we set the latch of
	 * some arbitrary process.
	 */
	bgwprocno = INT_ACCESS_ONCE(StrategyControl->bgwprocno);
	if (bgwprocno != -1)
	{
		/* reset bgwprocno first, before setting the latch */
		StrategyControl->bgwprocno = -1;

		/*
		 * Not acquiring ProcArrayLock here which is slightly icky. It's
		 * actually fine because procLatch isn't ever freed, so we just can
		 * potentially set the wrong process' (or no process') latch.
		 */
		SetLatch(&ProcGlobal->allProcs[bgwprocno].procLatch);
	}

	/*
	 * We count buffer allocation requests so that the bgwriter can estimate
	 * the rate of buffer consumption.  Note that buffers recycled by a
	 * strategy object are intentionally not counted here.
	 */
	pg_atomic_fetch_add_u32(&StrategyControl->numBufferAllocs, 1);

	/*
	 * First check, without acquiring the lock, whether there's buffers in the
	 * freelist. Since we otherwise don't require the spinlock in every
	 * StrategyGetBuffer() invocation, it'd be sad to acquire it here -
	 * uselessly in most cases. That obviously leaves a race where a buffer is
	 * put on the freelist but we don't see the store yet - but that's pretty
	 * harmless, it'll just get used during the next buffer acquisition.
	 *
	 * If there's buffers on the freelist, acquire the spinlock to pop one
	 * buffer of the freelist. Then check whether that buffer is usable and
	 * repeat if not.
	 *
	 * Note that the freeNext fields are considered to be protected by the
	 * buffer_strategy_lock not the individual buffer spinlocks, so it's OK to
	 * manipulate them without holding the spinlock.
	 */
	if (StrategyControl->firstFreeBuffer >= 0)
	{
		while (true)
		{
			/* Acquire the spinlock to remove element from the freelist */
			SpinLockAcquire(&StrategyControl->buffer_strategy_lock);

			if (StrategyControl->firstFreeBuffer < 0)
			{
				SpinLockRelease(&StrategyControl->buffer_strategy_lock);
				break;
			}

			buf = GetBufferDescriptor(StrategyControl->firstFreeBuffer);
			int buf_idx = StrategyControl->firstFreeBuffer;
			Assert(buf->freeNext != FREENEXT_NOT_IN_LIST);

			/* Unconditionally remove buffer from freelist */
			StrategyControl->firstFreeBuffer = buf->freeNext;
			buf->freeNext = FREENEXT_NOT_IN_LIST;

			/*
			 * Release the lock so someone else can access the freelist while
			 * we check out this buffer.
			 */
			SpinLockRelease(&StrategyControl->buffer_strategy_lock);

			/*
			 * If the buffer is pinned or has a nonzero usage_count, we cannot
			 * use it; discard it and retry.  (This can only happen if VACUUM
			 * put a valid buffer in the freelist and then someone else used
			 * it before we got to it.  It's probably impossible altogether as
			 * of 8.3, but we'd better check anyway.)
			 */
			local_buf_state = LockBufHdr(buf);

			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
				&& BUF_STATE_GET_USAGECOUNT(local_buf_state) == 0)
			{
				UnlockBufHdr(buf, local_buf_state);
				/*
				 * First time population for s3-fifo queue
				 * If hash in ghost queue, add [hash, StrategyControl->firstFreeBuffer] to main queue
				 * Else add [hash, StrategyControl->firstFreeBuffer] to probationary queue and handle
				 * prob-main crossings
				*/
				SpinLockAcquire(&S3GhostQueue->ring_buffer_lock);
				SpinLockAcquire(&S3MainQueue->ring_buffer_lock);
				SpinLockAcquire(&S3ProbationaryQueue->ring_buffer_lock);

				bool searchGhostQueue = DeleteFromRingBuffer(S3GhostQueue, tagHash, RB_BY_TAG);

				if (buf_idx == -1) elog(ERROR, "S3-FIFO: Negative index encountered on free list");

				if (searchGhostQueue) {
					// in ghost queue, add to main
					int evictedFromMain = FifoReinsertion(S3MainQueue, tagHash, buf_idx);

					if (evictedFromMain != -1) {
						SpinLockAcquire(&S3OrphanBufferIndexes->lock);
						S3OrphanBufferIndexes->isIdxFree[evictedFromMain] = true;
						SpinLockRelease(&S3OrphanBufferIndexes->lock);
					}
				} else {
					// not in ghost queue, add to probationary
					RingBufferEntry evictedFromProbationary = RingBufferEnqueue(S3ProbationaryQueue, tagHash, buf_idx);

					// Handle prob-main crossing if something was evicted from prob
					if (evictedFromProbationary.bufferDescIndex != -1) {

						ReferenceUsagePair metrics = GetRefUsageCount(evictedFromProbationary.bufferDescIndex);
						// If eviction has usage count > 0, fifo reinsert to main
						if (metrics.refCount > 0 || metrics.usageCount > 0) {
							FifoReinsertion(S3MainQueue, evictedFromProbationary.tag, evictedFromProbationary.bufferDescIndex);
							ZeroUsageCount(evictedFromProbationary.bufferDescIndex);
						} 
						else { // else add tag to ghost queue
							RingBufferEnqueue(S3GhostQueue, evictedFromProbationary.tag, -1);

							// Add orphaned index back onto freelist
							SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
							BufferDesc* freeBuffer = GetBufferDescriptor(evictedFromProbationary.bufferDescIndex);
							if (freeBuffer->freeNext == FREENEXT_NOT_IN_LIST) {
								freeBuffer->freeNext = StrategyControl->firstFreeBuffer;
								if (freeBuffer->freeNext < 0) StrategyControl->lastFreeBuffer = freeBuffer->buf_id;
								StrategyControl->firstFreeBuffer = freeBuffer->buf_id;
							}
							SpinLockRelease(&StrategyControl->buffer_strategy_lock);
						}
					}
				}

				SpinLockRelease(&S3GhostQueue->ring_buffer_lock);
				SpinLockRelease(&S3MainQueue->ring_buffer_lock);
				SpinLockRelease(&S3ProbationaryQueue->ring_buffer_lock);

				local_buf_state = LockBufHdr(buf);
				// if (strategy != NULL)
				// 	AddBufferToRing(strategy, buf);
				*buf_state = local_buf_state;
				return buf;
			}
			UnlockBufHdr(buf, local_buf_state);
		}
	}

	/* 
	 * If hash in ghost queue, remove from ghost and add [hash, buffer descriptor of evicted] to head of main
	 * Else add to head of probationary
	 * Also need to handle probationary-main crossings
	 * Add evicted item hash to ghost queue
	 * Return GetBufferDescriptor(bufferdescindex) of evicted item from end of probationary/main queue
	 *        Use main if in ghost or evicted item from  prob has usage count > 0
	 *        Use probationary if not in ghost and evicted item from prob has usage count 0
	*/
	SpinLockAcquire(&S3GhostQueue->ring_buffer_lock);
	SpinLockAcquire(&S3MainQueue->ring_buffer_lock);
	SpinLockAcquire(&S3ProbationaryQueue->ring_buffer_lock);

	bool searchGhostQueue = DeleteFromRingBuffer(S3GhostQueue, tagHash, RB_BY_TAG);

	int evictedIdx = -1;

	if (searchGhostQueue) {
		// in ghost queue, so add to main
		evictedIdx = FifoReinsertion(S3MainQueue, tagHash, -1);
		int idxAfterReordering = SearchRingBuffer(S3MainQueue, -1, RB_BY_INDEX);
		if (idxAfterReordering == -1) elog(ERROR, "S3-FIFO: New entry lost in main queue");
		if (evictedIdx == -1) elog(ERROR, "S3-FIFO: Negative index entered main queue");

		S3MainQueue->queue[idxAfterReordering].bufferDescIndex = evictedIdx;
		ZeroUsageCount(evictedIdx);
	} else {
		// not in ghost queue, add to probationary
		RingBufferEntry evictedFromProbationary = RingBufferEnqueue(S3ProbationaryQueue, tagHash, -1);

		if (evictedFromProbationary.bufferDescIndex != -1) {
			ReferenceUsagePair metrics = GetRefUsageCount(evictedFromProbationary.bufferDescIndex);

			// If eviction has usage count > 0, fifo reinsert to main
			if (metrics.refCount > 0 || metrics.usageCount > 0) {
				evictedIdx = FifoReinsertion(S3MainQueue, evictedFromProbationary.tag, evictedFromProbationary.bufferDescIndex);
				ZeroUsageCount(evictedFromProbationary.bufferDescIndex);
			}
			else { // else add tag to ghost queue
				RingBufferEnqueue(S3GhostQueue, evictedFromProbationary.tag, -1);
				ZeroUsageCount(evictedFromProbationary.bufferDescIndex);
				evictedIdx = evictedFromProbationary.bufferDescIndex;
			}

			if (evictedIdx == -1) elog(ERROR, "S3-FIFO: Negative index entered probationary queue");

			// Write buffer desc idx to probationary
			int idxAfterReordering = SearchRingBuffer(S3ProbationaryQueue, -1, RB_BY_INDEX);
			if (idxAfterReordering == -1) elog(ERROR, "S3-FIFO: New entry lost in probationary queue");
			S3ProbationaryQueue->queue[idxAfterReordering].bufferDescIndex = evictedIdx; 
		} else {
			elog(ERROR, "S3-FIFO: Negative index evicted from probationary queue");
		}
	}

	// DEBUG
	// Only enable when testing, these functions take (relatively) ages to execute
	// Enabling these WILL cause tests to run unbearably slow if using the default shared buffer size
	CheckIdxRange(S3MainQueue);
	CheckIdxRange(S3ProbationaryQueue);
	CheckForSharedIdxs(S3MainQueue, S3ProbationaryQueue);
	CheckForDuplicateIdxs(S3MainQueue);
	CheckForDuplicateIdxs(S3ProbationaryQueue);

	// ereport(LOG, errmsg("S3 Prob AFTER %d %d %d %d %d %d %d %d %d %d %d %d %d", 
	// 	S3ProbationaryQueue->queue[0].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[1].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[2].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[3].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[4].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[5].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[6].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[7].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[8].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[9].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[10].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[11].bufferDescIndex,
	// 	S3ProbationaryQueue->queue[12].bufferDescIndex
	// ));

	SpinLockRelease(&S3GhostQueue->ring_buffer_lock);
	SpinLockRelease(&S3MainQueue->ring_buffer_lock);
	SpinLockRelease(&S3ProbationaryQueue->ring_buffer_lock);

	if (evictedIdx == -1) elog(ERROR, "S3-FIFO: Tried to return negative index");
	buf = GetBufferDescriptor(evictedIdx);
	local_buf_state = LockBufHdr(buf);
	*buf_state = local_buf_state;
	return buf;


	// /* Nothing on the freelist, so run the "clock sweep" algorithm */
	// trycounter = NBuffers;
	// for (;;)
	// {
	// 	buf = GetBufferDescriptor(ClockSweepTick());

	// 	/*
	// 	 * If the buffer is pinned or has a nonzero usage_count, we cannot use
	// 	 * it; decrement the usage_count (unless pinned) and keep scanning.
	// 	 */
	// 	local_buf_state = LockBufHdr(buf);

	// 	if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
	// 	{
	// 		if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
	// 		{
	// 			local_buf_state -= BUF_USAGECOUNT_ONE;

	// 			trycounter = NBuffers;
	// 		}
	// 		else
	// 		{
	// 			/* Found a usable buffer */
	// 			if (strategy != NULL)
	// 				AddBufferToRing(strategy, buf);
	// 			*buf_state = local_buf_state;
	// 			return buf;
	// 		}
	// 	}
	// 	else if (--trycounter == 0)
	// 	{
	// 		/*
	// 		 * We've scanned all the buffers without making any state changes,
	// 		 * so all the buffers are pinned (or were when we looked at them).
	// 		 * We could hope that someone will free one eventually, but it's
	// 		 * probably better to fail than to risk getting stuck in an
	// 		 * infinite loop.
	// 		 */
	// 		UnlockBufHdr(buf, local_buf_state);
	// 		elog(ERROR, "no unpinned buffers available");
	// 	}
	// 	UnlockBufHdr(buf, local_buf_state);
	// }
}

/*
 * StrategyFreeBuffer: put a buffer on the freelist
 */
void
StrategyFreeBuffer(BufferDesc *buf)
{
	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	// SpinLockAcquire(&S3MainQueue->ring_buffer_lock);
	// SpinLockAcquire(&S3ProbationaryQueue->ring_buffer_lock);
	/*
	 * It is possible that we are told to put something in the freelist that
	 * is already in it; don't screw up the list if so.
	 */

	if (buf->freeNext == FREENEXT_NOT_IN_LIST)
	{
		// buf->freeNext = StrategyControl->firstFreeBuffer;
		// if (buf->freeNext < 0)
		// 	StrategyControl->lastFreeBuffer = buf->buf_id;
		// StrategyControl->firstFreeBuffer = buf->buf_id;

		// RingBufferDeleteIdx(S3MainQueue, buf->buf_id);
		// RingBufferDeleteIdx(S3ProbationaryQueue, buf->buf_id);

		// Edge case for when VACUUM puts a valid buffer in the freelist, ensures FIFO-reinsertion can return something in this case
		SpinLockAcquire(&S3OrphanBufferIndexes->lock);
		S3OrphanBufferIndexes->isIdxFree[buf->buf_id] = true;
		SpinLockRelease(&S3OrphanBufferIndexes->lock);
	}

	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
	// SpinLockRelease(&S3MainQueue->ring_buffer_lock);
	// SpinLockRelease(&S3ProbationaryQueue->ring_buffer_lock);
}

/*
 * StrategySyncStart -- tell BufferSync where to start syncing
 *
 * The result is the buffer index of the best buffer to sync first.
 * BufferSync() will proceed circularly around the buffer array from there.
 *
 * In addition, we return the completed-pass count (which is effectively
 * the higher-order bits of nextVictimBuffer) and the count of recent buffer
 * allocs if non-NULL pointers are passed.  The alloc count is reset after
 * being read.
 */
int
StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc)
{
	uint32		nextVictimBuffer;
	int			result;

	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	nextVictimBuffer = pg_atomic_read_u32(&StrategyControl->nextVictimBuffer);
	result = nextVictimBuffer % NBuffers;

	if (complete_passes)
	{
		*complete_passes = StrategyControl->completePasses;

		/*
		 * Additionally add the number of wraparounds that happened before
		 * completePasses could be incremented. C.f. ClockSweepTick().
		 */
		*complete_passes += nextVictimBuffer / NBuffers;
	}

	if (num_buf_alloc)
	{
		*num_buf_alloc = pg_atomic_exchange_u32(&StrategyControl->numBufferAllocs, 0);
	}
	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
	return result;
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * If bgwprocno isn't -1, the next invocation of StrategyGetBuffer will
 * set that latch.  Pass -1 to clear the pending notification before it
 * happens.  This feature is used by the bgwriter process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
void
StrategyNotifyBgWriter(int bgwprocno)
{
	/*
	 * We acquire buffer_strategy_lock just to ensure that the store appears
	 * atomic to StrategyGetBuffer.  The bgwriter should call this rather
	 * infrequently, so there's no performance penalty from being safe.
	 */
	SpinLockAcquire(&StrategyControl->buffer_strategy_lock);
	StrategyControl->bgwprocno = bgwprocno;
	SpinLockRelease(&StrategyControl->buffer_strategy_lock);
}


/*
 * StrategyShmemSize
 *
 * estimate the size of shared memory used by the freelist-related structures.
 *
 * Note: for somewhat historical reasons, the buffer lookup hashtable size
 * is also determined here.
 */
Size
StrategyShmemSize(void)
{
	Size		size = 0;

	/* size of lookup hash table ... see comment in StrategyInitialize */
	size = add_size(size, BufTableShmemSize(NBuffers + NUM_BUFFER_PARTITIONS));

	/* size of the shared replacement strategy control block */
	size = add_size(size, MAXALIGN(sizeof(BufferStrategyControl)));

	return size;
}

/*
 * StrategyInitialize -- initialize the buffer cache replacement
 *		strategy.
 *
 * Assumes: All of the buffers are already built into a linked list.
 *		Only called by postmaster and only during initialization.
 */
void
StrategyInitialize(bool init)
{
	bool		found;
	ereport(LOG, errmsg("Strategy INIT"));
	/*
	 * Initialize the shared buffer lookup hashtable.
	 *
	 * Since we can't tolerate running out of lookup table entries, we must be
	 * sure to specify an adequate table size here.  The maximum steady-state
	 * usage is of course NBuffers entries, but BufferAlloc() tries to insert
	 * a new entry before deleting the old.  In principle this could be
	 * happening in each partition concurrently, so we could need as many as
	 * NBuffers + NUM_BUFFER_PARTITIONS entries.
	 */
	InitBufTable(NBuffers + NUM_BUFFER_PARTITIONS);

	/*
	 * Get or create the shared strategy control block
	 */
	StrategyControl = (BufferStrategyControl *)
		ShmemInitStruct("Buffer Strategy Status",
						sizeof(BufferStrategyControl),
						&found);

	int mainQueueSize = (NBuffers * 90) / 100;
	S3MainQueue = (RingBuffer *)
		ShmemInitStruct("S3-FIFO main queue",
						offsetof(RingBuffer, queue) + (mainQueueSize * sizeof(RingBufferEntry)),
						&found);

	S3ProbationaryQueue = (RingBuffer *)
		ShmemInitStruct("S3-FIFO probationary queue",
						offsetof(RingBuffer, queue) + ((NBuffers - mainQueueSize) * sizeof(RingBufferEntry)),
						&found);

	S3GhostQueue = (RingBuffer *)
		ShmemInitStruct("S3-FIFO ghost queue",
						offsetof(RingBuffer, queue) + (mainQueueSize * sizeof(RingBufferEntry)),
						&found);
	
	S3OrphanBufferIndexes = (OrphanBufferArray *)
		ShmemInitStruct("S3-FIFO buffer indexes orphaned by eviction from probationary queue",
						offsetof(OrphanBufferArray, isIdxFree) + (NBuffers * sizeof(bool)),
						&found);

	if (!found)
	{
		ereport(LOG, errmsg("Control structures not found"));
		/*
		 * Only done once, usually in postmaster
		 */
		Assert(init);

		SpinLockInit(&StrategyControl->buffer_strategy_lock);

		/*
		 * Grab the whole linked list of free buffers for our strategy. We
		 * assume it was previously set up by BufferManagerShmemInit().
		 */
		StrategyControl->firstFreeBuffer = 0;
		StrategyControl->lastFreeBuffer = NBuffers - 1;

		/* Initialize the clock sweep pointer */
		pg_atomic_init_u32(&StrategyControl->nextVictimBuffer, 0);

		/* Clear statistics */
		StrategyControl->completePasses = 0;
		pg_atomic_init_u32(&StrategyControl->numBufferAllocs, 0);

		/* No pending notification */
		StrategyControl->bgwprocno = -1;

		// INITIALIZE S3-FIFO QUEUES
		SpinLockInit(&S3MainQueue->ring_buffer_lock);
		S3MainQueue->head = 0;
		S3MainQueue->tail = 0;
		S3MainQueue->currentSize = 0;
		S3MainQueue->maxSize = mainQueueSize;
		for (int i = 0; i < S3MainQueue->maxSize; i++) {
			S3MainQueue->queue[i].tag = -1;
			S3MainQueue->queue[i].bufferDescIndex = -1;
		}

		SpinLockInit(&S3ProbationaryQueue->ring_buffer_lock);
		S3ProbationaryQueue->head = 0;
		S3ProbationaryQueue->tail = 0;
		S3ProbationaryQueue->currentSize = 0;
		S3ProbationaryQueue->maxSize = NBuffers - mainQueueSize;
		for (int i = 0; i < S3ProbationaryQueue->maxSize; i++) {
			S3ProbationaryQueue->queue[i].tag = -1;
			S3ProbationaryQueue->queue[i].bufferDescIndex = -1;
		}

		SpinLockInit(&S3GhostQueue->ring_buffer_lock);
		S3GhostQueue->head = 0;
		S3GhostQueue->tail = 0;
		S3GhostQueue->currentSize = 0;
		S3GhostQueue->maxSize = mainQueueSize;
		for (int i = 0; i < S3GhostQueue->maxSize; i++) {
			S3GhostQueue->queue[i].tag = -1;
			S3GhostQueue->queue[i].bufferDescIndex = -1;
		}

		SpinLockInit(&S3OrphanBufferIndexes->lock);
		for (int i = 0; i < NBuffers; i++) {
			S3OrphanBufferIndexes->isIdxFree[i] = false;
		}
	}
	else
		Assert(!init);
}


/* ----------------------------------------------------------------
 *				Backend-private buffer ring management
 * ----------------------------------------------------------------
 */


/*
 * GetAccessStrategy -- create a BufferAccessStrategy object
 *
 * The object is allocated in the current memory context.
 */
BufferAccessStrategy
GetAccessStrategy(BufferAccessStrategyType btype)
{
	int			ring_size_kb;

	/*
	 * Select ring size to use.  See buffer/README for rationales.
	 *
	 * Note: if you change the ring size for BAS_BULKREAD, see also
	 * SYNC_SCAN_REPORT_INTERVAL in access/heap/syncscan.c.
	 */
	switch (btype)
	{
		case BAS_NORMAL:
			/* if someone asks for NORMAL, just give 'em a "default" object */
			return NULL;

		case BAS_BULKREAD:
			ring_size_kb = 256;
			break;
		case BAS_BULKWRITE:
			ring_size_kb = 16 * 1024;
			break;
		case BAS_VACUUM:
			ring_size_kb = 2048;
			break;

		default:
			elog(ERROR, "unrecognized buffer access strategy: %d",
				 (int) btype);
			return NULL;		/* keep compiler quiet */
	}

	return GetAccessStrategyWithSize(btype, ring_size_kb);
}

/*
 * GetAccessStrategyWithSize -- create a BufferAccessStrategy object with a
 *		number of buffers equivalent to the passed in size.
 *
 * If the given ring size is 0, no BufferAccessStrategy will be created and
 * the function will return NULL.  ring_size_kb must not be negative.
 */
BufferAccessStrategy
GetAccessStrategyWithSize(BufferAccessStrategyType btype, int ring_size_kb)
{
	int			ring_buffers;
	BufferAccessStrategy strategy;

	Assert(ring_size_kb >= 0);

	/* Figure out how many buffers ring_size_kb is */
	ring_buffers = ring_size_kb / (BLCKSZ / 1024);

	/* 0 means unlimited, so no BufferAccessStrategy required */
	if (ring_buffers == 0)
		return NULL;

	/* Cap to 1/8th of shared_buffers */
	ring_buffers = Min(NBuffers / 8, ring_buffers);

	/* NBuffers should never be less than 16, so this shouldn't happen */
	Assert(ring_buffers > 0);

	/* Allocate the object and initialize all elements to zeroes */
	strategy = (BufferAccessStrategy)
		palloc0(offsetof(BufferAccessStrategyData, buffers) +
				ring_buffers * sizeof(Buffer));

	/* Set fields that don't start out zero */
	strategy->btype = btype;
	strategy->nbuffers = ring_buffers;

	return strategy;
}

/*
 * GetAccessStrategyBufferCount -- an accessor for the number of buffers in
 *		the ring
 *
 * Returns 0 on NULL input to match behavior of GetAccessStrategyWithSize()
 * returning NULL with 0 size.
 */
int
GetAccessStrategyBufferCount(BufferAccessStrategy strategy)
{
	if (strategy == NULL)
		return 0;

	return strategy->nbuffers;
}

/*
 * GetAccessStrategyPinLimit -- get cap of number of buffers that should be pinned
 *
 * When pinning extra buffers to look ahead, users of a ring-based strategy are
 * in danger of pinning too much of the ring at once while performing look-ahead.
 * For some strategies, that means "escaping" from the ring, and in others it
 * means forcing dirty data to disk very frequently with associated WAL
 * flushing.  Since external code has no insight into any of that, allow
 * individual strategy types to expose a clamp that should be applied when
 * deciding on a maximum number of buffers to pin at once.
 *
 * Callers should combine this number with other relevant limits and take the
 * minimum.
 */
int
GetAccessStrategyPinLimit(BufferAccessStrategy strategy)
{
	if (strategy == NULL)
		return NBuffers;

	switch (strategy->btype)
	{
		case BAS_BULKREAD:

			/*
			 * Since BAS_BULKREAD uses StrategyRejectBuffer(), dirty buffers
			 * shouldn't be a problem and the caller is free to pin up to the
			 * entire ring at once.
			 */
			return strategy->nbuffers;

		default:

			/*
			 * Tell caller not to pin more than half the buffers in the ring.
			 * This is a trade-off between look ahead distance and deferring
			 * writeback and associated WAL traffic.
			 */
			return strategy->nbuffers / 2;
	}
}

/*
 * FreeAccessStrategy -- release a BufferAccessStrategy object
 *
 * A simple pfree would do at the moment, but we would prefer that callers
 * don't assume that much about the representation of BufferAccessStrategy.
 */
void
FreeAccessStrategy(BufferAccessStrategy strategy)
{
	/* don't crash if called on a "default" strategy */
	if (strategy != NULL)
		pfree(strategy);
}

/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the
 *		ring is empty / not usable.
 *
 * The bufhdr spin lock is held on the returned buffer.
 */
static BufferDesc *
GetBufferFromRing(BufferAccessStrategy strategy, uint32 *buf_state)
{
	BufferDesc *buf;
	Buffer		bufnum;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */


	/* Advance to next ring slot */
	if (++strategy->current >= strategy->nbuffers)
		strategy->current = 0;

	/*
	 * If the slot hasn't been filled yet, tell the caller to allocate a new
	 * buffer with the normal allocation strategy.  He will then fill this
	 * slot by calling AddBufferToRing with the new buffer.
	 */
	bufnum = strategy->buffers[strategy->current];
	if (bufnum == InvalidBuffer)
		return NULL;

	/*
	 * If the buffer is pinned we cannot use it under any circumstances.
	 *
	 * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
	 * since our own previous usage of the ring element would have left it
	 * there, but it might've been decremented by clock sweep since then). A
	 * higher usage_count indicates someone else has touched the buffer, so we
	 * shouldn't re-use it.
	 */
	buf = GetBufferDescriptor(bufnum - 1);
	local_buf_state = LockBufHdr(buf);
	if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
		&& BUF_STATE_GET_USAGECOUNT(local_buf_state) <= 1)
	{
		*buf_state = local_buf_state;
		return buf;
	}
	UnlockBufHdr(buf, local_buf_state);

	/*
	 * Tell caller to allocate a new buffer with the normal allocation
	 * strategy.  He'll then replace this ring element via AddBufferToRing.
	 */
	return NULL;
}

/*
 * AddBufferToRing -- add a buffer to the buffer ring
 *
 * Caller must hold the buffer header spinlock on the buffer.  Since this
 * is called with the spinlock held, it had better be quite cheap.
 */
static void
AddBufferToRing(BufferAccessStrategy strategy, BufferDesc *buf)
{
	strategy->buffers[strategy->current] = BufferDescriptorGetBuffer(buf);
}

/*
 * Utility function returning the IOContext of a given BufferAccessStrategy's
 * strategy ring.
 */
IOContext
IOContextForStrategy(BufferAccessStrategy strategy)
{
	if (!strategy)
		return IOCONTEXT_NORMAL;

	switch (strategy->btype)
	{
		case BAS_NORMAL:

			/*
			 * Currently, GetAccessStrategy() returns NULL for
			 * BufferAccessStrategyType BAS_NORMAL, so this case is
			 * unreachable.
			 */
			pg_unreachable();
			return IOCONTEXT_NORMAL;
		case BAS_BULKREAD:
			return IOCONTEXT_BULKREAD;
		case BAS_BULKWRITE:
			return IOCONTEXT_BULKWRITE;
		case BAS_VACUUM:
			return IOCONTEXT_VACUUM;
	}

	elog(ERROR, "unrecognized BufferAccessStrategyType: %d", strategy->btype);
	pg_unreachable();
}

/*
 * StrategyRejectBuffer -- consider rejecting a dirty buffer
 *
 * When a nondefault strategy is used, the buffer manager calls this function
 * when it turns out that the buffer selected by StrategyGetBuffer needs to
 * be written out and doing so would require flushing WAL too.  This gives us
 * a chance to choose a different victim.
 *
 * Returns true if buffer manager should ask for a new victim, and false
 * if this buffer should be written and re-used.
 */
bool
StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc *buf, bool from_ring)
{
	/* We only do this in bulkread mode */
	if (strategy->btype != BAS_BULKREAD)
		return false;

	/* Don't muck with behavior of normal buffer-replacement strategy */
	if (!from_ring ||
		strategy->buffers[strategy->current] != BufferDescriptorGetBuffer(buf))
		return false;

	/*
	 * Remove the dirty buffer from the ring; necessary to prevent infinite
	 * loop if all ring members are dirty.
	 */
	strategy->buffers[strategy->current] = InvalidBuffer;

	return true;
}
