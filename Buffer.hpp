// Buffer.h

#ifndef _BUFFER_HPP
#define _BUFFER_HPP

#ifdef _OPENMP
#include "omp.h"
#else
int omp_get_thread_num() { return 0; }
int omp_get_num_threads() { return 1; }
#endif

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <sstream>

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/stack.hpp>
#include <boost/shared_ptr.hpp>

#define LOG(msg) { std::stringstream s; s << "T" << omp_get_thread_num() << ": " << msg << std::endl; std::string str = s.str(); std::cerr << str; }

class Buffer {
public:
	typedef char* charPtr;
	typedef int32_t Size;
	const static Size DefaultSize = 8192;

	Buffer(Size size = DefaultSize) : _buf(NULL), _gptr(NULL), _pptr(NULL), _mark(0), _capacity(0) {
		resize(size);
	}
	~Buffer() {
		reset();
	}

	// rewind pointers to mark (default beginning), keep memory allocated
	void clear(Size mark = 0) {
		assert( validate() );
		assert( mark <= size() );
		assert( _pptr - _buf >= mark );
		_gptr = _buf;
		_pptr = _buf + mark;
		_mark = mark;
	}
	bool empty() const {
		assert( validate() );
		return _gptr == _buf && _pptr == _buf && _mark == 0;
	}

	// alter capacity.  Can only decrease down to size()
	void resize(Size newsize) {
		assert( _capacity == 0 || validate() );
		if (newsize == _capacity)
			return;
		Size glen = _gptr - _buf;
		Size plen = size();
		if (glen >= newsize || plen >= newsize) {
			return;
		}
		_buf = (charPtr) realloc(_buf, newsize);
		_gptr = _buf + glen;
		_pptr = _buf + plen;
		_capacity = newsize;
		assert( validate() );
	}

	// write up to capacity()
	Size write(const char *src, Size len) {
		assert( validate() );
		len = std::min(len, premainder());
		//LOG( (long) this << "-write(" << len << "): remaining " << premainder() << " with " << (int) (_pptr - _buf) << " or " << size() << " mark " << _mark );
		if (len > 0)
			memcpy(_pptr, src, len);
		_pptr += len;	
		assert( validate() );
		//LOG( (long) this << "-wrote: " << len << " " << getState() );
		return len;
	}

	// read some bytes from buffer
	Size read(char *dst, Size len) {
		len = std::min(len, gremainder());
		memcpy(dst, _gptr, len);
		_gptr += len;
		assert( validate() );
		return len;
	}

	// set marker deliminating the end of a logical block
	// return the number of bytes since the last mark
	Size setMark() {
		assert( validate() );
		Size oldMark = _mark;
		_mark = size();
		//LOG( (long) this << "-setMark: " << _mark << ": " << (int) (_pptr - _buf) << " old: " << oldMark );
		assert(_mark >= oldMark);
		return _mark - oldMark;
	}

	// return the last mark that was set
	Size getMark() const {
		return _mark;
	}
	// the capacity of the buffer
	Size capacity() const {
		return _capacity;
	}

	// raw iterators
	charPtr begin() {
		return _buf;
	}
	const charPtr begin() const {
		return _buf;
	}
	const charPtr end() const {
		return _buf + _capacity;
	}

	// iterator for data region ready for gets
	charPtr &gbegin() {
		return _gptr;
	}
	const charPtr gbegin() const {
		return _gptr;
	}
	const charPtr gend() const {
		assert( validate() );
		return _pptr;
	}

	// iterator for empty region ready for puts
	charPtr &pbegin() {
		return _pptr;
	}
	const charPtr pbegin() const {
		return _pptr;
	}
	const charPtr pend() const {
		return end();
	}

	// iterator starting at last mark until the end of the put data
	const charPtr beginMark() const {
		assert( validate() );
		return _buf + _mark;
	}
	const charPtr endMark() const {
		return _pptr;
	}
	
	// bytes written past last mark
	Size markRemainder() const {
		assert( validate() );
		return endMark() - beginMark();
	}

	// bytes remaining in capacity
	Size premainder() const {
		assert( validate() );
		return pend() - pbegin();
	}
	Size gremainder() const {
		return gend() - gbegin();
	}

	Size greturned() const {
		return gbegin() - begin();
	}

	// bytes written
	Size size() const {
		return _buf == NULL ? 0 : _pptr - _buf;
	}

	charPtr gbump(Size bytes) {
		assert( validate() );
		_gptr += bytes;
		//LOG((long) this << " Buffer::gbump(" << bytes << "): size:" << size() << " remainder: " << gremainder());
		if ( !gvalidate() )
			throw;
		return _gptr;
	}
	charPtr pbump(Size bytes) {
		assert( validate() );
		_pptr += bytes;
		//LOG((long) this << " Buffer::pbump(" << bytes << "): size:" << size() << " remainder: " << premainder());
		if ( !pvalidate() )
			throw;
		return _pptr;
	}

	void setg (char* gbeg, char* gnext, char* _gend) {
		_gptr = gnext;
		//LOG((long) this << " Buffer::setg(" << (long) gbeg << "): size:" << (long) gnext);
		if (gbeg != _buf || (_gend != end() || _gend != gend()) || !gvalidate())
			throw;
	}
	void setp (char* new_pbase, char* new_epptr) {
		_pptr = _buf;
		//LOG((long) this << " Buffer::setp(" << (long) new_pbase << "): size:" << (long) _buf);
		if (new_pbase != _buf || new_epptr != end() || !pvalidate())
			throw;
	}
	void swap(Buffer &rhs) {
		std::swap(_buf, rhs._buf);
		std::swap(_gptr, rhs._gptr);
		std::swap(_pptr, rhs._pptr);
		std::swap(_mark, rhs._mark);
		std::swap(_capacity, rhs._capacity);
	}

	std::string getState() const {
		std::stringstream ss;
		ss << "Buffer::getState(): " << (long) this << " get: " << (_gptr - _buf) << ", put: " << (_pptr - _buf) << ", mark: " << _mark << ", cap: " << _capacity;
		return ss.str();
	}

	Size getGetBufferUsed() const {
		return (_pptr - _gptr);
	}

	Size getPutBufferUsed() const {
		return (_pptr - _buf);
	}

protected:
	// release memory
	void reset() {
		free(_buf);
		_buf = _gptr = _pptr = NULL;
		_mark = _capacity = 0;
	}

	// sanity check for assertions
	bool validate() const {
		Size s = size();
		return ((_buf != NULL) & (_capacity >= _mark) & (_capacity >= s) & (s >= 0) & (_mark >= 0) & (s >= _mark));
	}
	bool gvalidate() const {
		return (_gptr - _buf >= 0 && _gptr - _buf <= _capacity && _pptr - _gptr >= 0);
	}
	bool pvalidate() const {
		return (_pptr - _buf >= 0 && _pptr - _buf <= _capacity);
	}
	
private:
	// Note: could refactor to be 24bytes, not 32bytes
	charPtr _buf, _gptr, _pptr;
	Size _mark, _capacity;

};

class BufferPool {
public:
	typedef Buffer::Size Size;
	typedef Buffer* BufferPtr;
	typedef boost::lockfree::stack< BufferPtr > Stack;
	typedef boost::shared_ptr< Stack > StackPtr;
	BufferPool(int capacity = 8, Size bufferSize = Buffer::DefaultSize) 
		: _stack(new Stack( capacity )), _bufferSize(bufferSize), _allocCount(0), _deallocCount(0) {}
	~BufferPool() {
		clear();
	}
	void clear() {
		BufferPtr p = NULL;
		while ( _stack->pop(p) ) {
			delete p;
			p = NULL;
			_deallocCount++;
		}
	}
	BufferPtr getBuffer() {
		BufferPtr p = NULL;
		if (_stack->pop(p)) {
			return p;
		} else {
			_allocCount++;
			return new Buffer(_bufferSize);
		}
	}
	bool putBuffer(BufferPtr &p) {
		assert(p != NULL);
		p->clear(); // only return clean buffers
		bool ret = _stack->bounded_push(p);
		if (!ret) {
			delete p;
			p = NULL;
			_deallocCount++;
		}
		return ret;
	}
	Size getBufferSize() const { return _bufferSize; }
	int64_t getAllocCount() const { return _allocCount; }
	int64_t getDeallocCount() const { return _deallocCount; }

	void swap(BufferPool &rhs) {
		std::swap(_stack, rhs._stack );
		std::swap(_bufferSize, rhs._bufferSize);
		int64_t tmp = _allocCount.load();
		_allocCount = rhs._allocCount.load();
		rhs._allocCount.store( tmp );

		tmp = _deallocCount.load();
		_deallocCount.store( rhs._deallocCount.load() );
		rhs._deallocCount.store( tmp );
	}	

private:
	StackPtr _stack;
	Size _bufferSize;
	boost::atomic<int64_t> _allocCount, _deallocCount;
};

class BufferFifo {
public:
	typedef Buffer::Size Size;
	typedef Buffer* BufferPtr;
	typedef boost::lockfree::queue< BufferPtr > Queue;
	typedef boost::shared_ptr< Queue > QueuePtr;
	BufferFifo(int numBuffers = 16, Size bufferSize = Buffer::DefaultSize, int poolMultiplier = 3) 
		: _queue(new Queue(numBuffers) ), _pool(numBuffers*poolMultiplier, bufferSize),
		  _totalReaders(0), _closedReaders(0), _totalWriters(0), _closedWriters(0),
		  _pushed(0), _popped(0), _isEOF(false) {}
	~BufferFifo() {
		clear();
	}
	
	void push(BufferPtr &p) {
		//std::cerr << this << "-BufferFifo::push(" << &p << "): " << p->getState() << std::endl;
		_pushed++;
		while(!_queue->push(p));
		_pushCond.notify_one();
		p = NULL;
	}
	bool pop(BufferPtr &p) {
		bool ret = false;
		while (!ret && !(_isEOF && empty())) {
			ret = _queue->pop(p);
		}
		if (ret) {
			_popCond.notify_one();
			_popped++;
		}
		//std::cerr << this << "-BufferFifo::pop(" << &p << "): " << ret << std::endl;
		return ret;
	}
	bool empty() const {
		return _queue->empty() && _pushed == _popped;
	}
	bool isEOF() const {
		return _isEOF && empty();
	}
	void setEOF() {
		if (_isEOF) {
			std::cerr << "WARNING: you should only setEOF once per program not per thread" << std::endl;
		}
		_isEOF = true;
		int count = getActiveWriterCount();
		if (count != 0) {
			std::cerr << "WARNING: there are still active writers (" << count << ") when setEOF() was called... Chaos shall follow" << std::endl;
		}
		_pushCond.notify_all();
	}

	BufferPool &getBufferPool() { return _pool; }

	void swap(BufferFifo &rhs) {
		std::swap(_queue, rhs._queue);
		_pool.swap(rhs._pool);
	}
	boost::mutex &getPushMutex() {
		return _pushMutex;
	}
	boost::mutex &getPopMutex() {
		return _popMutex;
	}
	boost::condition_variable &getPushCondtion() {
		return _pushCond;
	}
	boost::condition_variable &getPopCondition() {
		return _popCond;
	}
	int registerReader() {
		return ++_closedReaders;
	}
	int deregisterReader() {
		return ++_totalReaders;
	}
	int registerWriter() {
		return ++_totalWriters;
	}
	int deregisterWriter() {
		return ++_closedWriters;
	}
	int getWriterCount() const {
		return _totalWriters.load();
	}
	int getActiveWriterCount() const {
		return _totalWriters.load() - _closedWriters.load();
	}
	int getReaderCount() const {
		return _totalReaders.load();
	}
	int getActiveReaderCount() const {
		return _totalReaders.load() - _closedReaders.load();
	}

protected:
	void clear() {
		BufferPtr p = NULL;
		while (_queue->pop(p)) {
			if (p != NULL) {
				std::cerr << "BufferFifo purging queue member: " << p << std::endl;
			}
			delete p;
			p = NULL;
		}
	}

private:
	QueuePtr _queue;
	BufferPool _pool;
	boost::atomic<int> _totalReaders, _closedReaders, _totalWriters, _closedWriters, _pushed, _popped;
	boost::mutex _pushMutex, _popMutex;
	boost::condition_variable _pushCond, _popCond;
	bool _isEOF;
};


#endif // _BUFFER_HPP
