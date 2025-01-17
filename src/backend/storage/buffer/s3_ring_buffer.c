#include "postgres.h"
#include "storage/buf_internals.h"

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

typedef struct 
{
	int refCount;
	int usageCount;
} ReferenceUsagePair;

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
static RingBufferEntry RingBufferPush(RingBuffer* rb, int tag, int idx) {
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

/*
 * Pops and returns an element from the ring buffer tail
 * If ring buffer is empty, return {-1, -1}
*/
static RingBufferEntry RingBufferPop(RingBuffer* rb) {
    RingBufferEntry popped = {.tag = -1, .bufferDescIndex = -1};

    if (IsRingBufferEmpty(rb)) {
        return popped;
    }

    popped.tag = rb->queue[rb->tail].tag;
    popped.bufferDescIndex = rb->queue[rb->tail].bufferDescIndex;

    rb->tail = (rb->tail + 1) % rb->maxSize;

    rb->currentSize--;

    return popped;
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
		RingBufferEntry enqueueResult = RingBufferPush(rb, newTag, newIdx);
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

	if (newIdx == -1) elog(ERROR, "S3-FIFO: No unpinned buffers available");

	return newIdx;
}
