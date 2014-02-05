// marked_iostream.hpp

#ifndef _MARKED_IOSTREAM_H_
#define _MARKED_IOSTREAM_H_

#include <cstdio>
#include <streambuf>
#include <iostream>
#include <cstring>

#include "Buffer.hpp"

// each thread should create its own marked_fifo_streambuf (and associated iostreams)
// using the same BufferFifo...
// the iostreams should call setMark() at regular (and frequent relative to bufferSize) intervals

class marked_fifo_streambuf : public std::streambuf {
public:
	typedef BufferPool::BufferPtr BufferPtr;
	typedef Buffer::Size Size;
	typedef std::streamsize streamsize;
	typedef std::streampos streampos;

	marked_fifo_streambuf(BufferFifo &bufFifo) 
		: std::streambuf(), _bufFifo(&bufFifo), _readOnly(false), _writeOnly(false) {
		_buf = _bufFifo->getBufferPool().getBuffer();
		setbuf(_buf->begin(), _buf->capacity());
	}
	virtual ~marked_fifo_streambuf() {
		sync();
		_bufFifo->getBufferPool().putBuffer(_buf);
		if (_readOnly)
			_bufFifo->deregisterReader();
		if (_writeOnly)
			_bufFifo->deregisterWriter();
	}

	int setMark(bool flush = false) {
		assert(_writeOnly);
		int lastMarkSize = _buf->setMark();
		if (flush || lastMarkSize >= _buf->premainder()) {
			overflow(EOF);
		}
		return lastMarkSize;
	}

protected:
	char* eback() const { setReadOnly(); return _buf->begin(); }
	char* gptr() const { setReadOnly(); return _buf->gbegin(); }
	char* egptr() const { setReadOnly(); return _buf->gend(); }
	void gbump(int n) {
		setReadOnly();
		_buf->gbump(n);
	}
	void setg (char* gbeg, char* gnext, char* gend) {
		setReadOnly();
		_buf->setg(gbeg, gnext, gend);
	}
	
	char* pbase() const { setWriteOnly(); return _buf->begin(); }
	char* pptr() const { setWriteOnly(); return _buf->pbegin(); }
	char* epptr() const { setWriteOnly(); return _buf->pend(); }
	void pbump(int n) {
		setWriteOnly();
		_buf->pbump(n);
	}
	void setp (char* new_pbase, char* new_epptr){
		setWriteOnly();
		 _buf->setp(new_pbase, new_epptr);
	}
	void swap(marked_fifo_streambuf &rhs) {
		std::swap(_bufFifo, rhs._bufFifo);
		std::swap(_buf, rhs._buf);
		std::swap(_readOnly, rhs._readOnly);
		std::swap(_writeOnly, rhs._writeOnly);
	}

	// virtual
	//using void imbue (const locale& loc);

	// buffer mgmt virtuals
	//using std::streambuf* setbuf (char* s, streamsize n);

	//using streampos seekoff (streamoff off, ios_base::seekdir way,
        //           ios_base::openmode which = ios_base::in | ios_base::out);

	//using streampos seekpos (streampos sp, ios_base::openmode which = ios_base::in | ios_base::out);

	int sync() {
		if (_writeOnly) 
			setMark(true);
		if (_readOnly && _buf->gremainder() == 0)
			underflow();
		return 0;
	};

	// get virtuals
	streamsize showmanyc() { 
		setReadOnly();
		return _buf->gremainder();
	}
	streamsize xsgetn (char* s, streamsize n) {
		setReadOnly();
		return _buf->read(s, n);
	}
	int underflow() {
		setReadOnly();
		//assert(_buf->gremainder() == 0);
		// get a new _buf from the fifo stream
		BufferPtr next = NULL;
		// get a new _buf from the fifo stream
		if (_bufFifo->pop(next)) {
			// put _buf back in the pool
			_bufFifo->getBufferPool().putBuffer(_buf);
			_buf = next;
		} // else keep this old, exhausted _buf active
		if (_buf->gremainder() == 0)
			return EOF;
		return *_buf->gbegin();
	}

	//using int uflow();
	//using int pbackfail (int c = EOF);

	// put virtuals
	streamsize xsputn (const char* s, streamsize n) {
		setWriteOnly();
		return _buf->write(s, n);
	}

	int overflow (int c = EOF) {
		setWriteOnly();
		// get a new Buffer from the pool
		BufferPtr next = _bufFifo->getBufferPool().getBuffer();

		// check for trailing bytes after the mark & move to next buffer
		int markRemainder = _buf->markRemainder();
		if (markRemainder > 0) {
			next->write(_buf->beginMark(), markRemainder);
			_buf->clear(_buf->getMark());
		}

		// push old to the fifo stream
		_bufFifo->push(_buf);

		// assign new buffer and optionally write the next char
		_buf = next;
		if (c != EOF) {
			char c1 = (char) c;
			_buf->write(&c1,1);
		}
		return c;
	}
	
private:
	inline void setReadOnly() const {
		assert(!_writeOnly);
		if (!_readOnly) {
			_bufFifo->registerReader();
			_readOnly = true;
		}
	}
	inline void setWriteOnly() const {
		assert(!_readOnly);
		if (!_writeOnly) {
			_bufFifo->registerWriter();
			_writeOnly = true;
		}
	}

private:
	BufferFifo *_bufFifo;
	BufferPtr _buf;
	mutable bool _readOnly, _writeOnly;
};

class marked_istream : public std::istream {
public:
	marked_istream(BufferFifo &bufFifo) 
		: std::istream( new marked_fifo_streambuf( bufFifo ) ) {}

	virtual ~marked_istream() {
		delete rdbuf();
	}
	marked_fifo_streambuf * rdbuf() {
		return (marked_fifo_streambuf *) ((std::istream*) this)->rdbuf();
	}
};

class marked_ostream : public std::ostream {
public:
	marked_ostream(BufferFifo &bufFifo) 
		: std::ostream( new marked_fifo_streambuf( bufFifo ) ) {}

	virtual ~marked_ostream() {
		delete rdbuf();
	}
	int setMark() {
		return rdbuf()->setMark();
	}
	marked_fifo_streambuf * rdbuf() {
		return (marked_fifo_streambuf *) ((std::istream*) this)->rdbuf();
	}
};


#endif // _MARKED_IOSTREAM_H_
