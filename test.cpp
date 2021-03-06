// module load boost/1.53.0
// g++ -Wall -g -fopenmp -I $BOOST_DIR/include -L $BOOST_DIR/lib test.cpp -lboost_system -lboost_thread

#include "Buffer.hpp"
#include "marked_iostream.hpp"

#ifdef _OPENMP
#include "omp.h"
#else
int omp_get_thread_num() { return 0; }
int omp_get_num_threads() { return 1; }
#endif

#include <stdio.h>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_smallint.hpp>
#include <boost/random/normal_distribution.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>

using namespace std;


class BlockBytes {
public:
	int32_t blockBytes;
	BlockBytes() : blockBytes(0) {}
	std::istream& read(std::istream &is) {
		assert(is.good());
		is.read((char *) &blockBytes, sizeof(blockBytes));
		assert(is.good());
		return is;
	}
	std::ostream& write(std::ostream &os) const {
		assert(os.good());
		os.write((const char*) &blockBytes, sizeof(blockBytes));
		//LOG("BlockBytes::write " << blockBytes << " os: " << (long) &os);
		assert(os.good());
		return os;
	}
	int32_t getBytes() const {
		return blockBytes;
	}
	void setBytes(int32_t bytes) {
		blockBytes = bytes;
	}
	void reset() {
		blockBytes = 0;
	}
};
class BlockId : public BlockBytes {
public:
	int32_t blockId;
	BlockId(): BlockBytes(), blockId(-1) {}
	std::istream& read(std::istream &is) {
		assert(is.good());
		BlockBytes::read(is);
		is.read((char *) &blockId, sizeof(blockId));
		assert(is.good());
		return is;
	}
	std::ostream& write(std::ostream &os) const {
		assert(os.good());
		BlockBytes::write(os);
		os.write((const char*) &blockId, sizeof(blockId));
		//LOG("BlockId::write " << blockId << " os: " << (long) &os);
		assert(os.good());
		return os;
	}
	void reset() {
		BlockBytes::reset();
		blockId = -1;
	}
};

template<typename MetaData = BlockBytes>
class BaseMessage {
public:
	char *data;
	MetaData metaData;

	BaseMessage(): data(NULL), metaData() {}
	BaseMessage(std::istream &is) : data(NULL), metaData() {
		read(is);
	}
	virtual ~BaseMessage() {
		free(data);
	}
	std::istream& read(std::istream &is) {
		assert(is.good());
		metaData.read(is);
		reserve(metaData.getBytes());
		is.read(data, metaData.getBytes());
		assert(is.good());
		return is;
	}
	std::ostream& write(std::ostream &os) const {
		assert(os.good());
		assert(metaData.getBytes() > 0);
		metaData.write(os);
		os.write(data, metaData.getBytes());
		//LOG("Message::write " << metaData.getBytes() << " os: " << (long) &os);
		assert(os.good());
		return os;
	}
	void reserve(int32_t n) {
		data = (char*) realloc(data, n);
		assert(data != NULL);
	}
	void setMessage(const char* src, int32_t n) {
		reserve(n);
		memcpy(data, src, n);
		metaData.setBytes(n);
	}
	void reset() {
		metaData.reset();
	}
	int32_t getBytes() const {
		return metaData.getBytes();
	}
	const MetaData &getMetaData() const {
		return metaData;
	}
};

class MessageTest {
public:
	char *data;
	MessageTest() : data(NULL) {
		reset();
	}
	~MessageTest() { free(data); }
	void reset() {
		reserve(sizeof(int32_t) * 2 + 8);
		getBytes() = 0;
		getId() = -1;
	}
	
	static int32_t getMessageOverhead() {
		return sizeof(int32_t) * 2;
	}
	int32_t &getBytes() {
		return *((int32_t*) data);
	}
	int32_t &getId() {
		return *(&getBytes() + 1);
	}
	char *getData() {
		return (char*) (&getId() + 1);
	}
	
	void reserve( int32_t size) {
		data = (char*) realloc(data, getMessageOverhead() + size);
	}
	void setMessage(int32_t id, int32_t size) {
		data = (char*) realloc(data, getMessageOverhead() + size);
		char c = (char) id;
		getBytes() = size;
		getId()  = id; 	
		char *d = getData();
		for(int32_t i = 0; i < size; i++) {
			d[i] = c;
		}
	}
	std::istream& read(std::istream &is) {
		is.read( (char*) data, getMessageOverhead() );
		reserve( getBytes() );
		is.read( getData(), getBytes());
		return is;
	}
	std::ostream& write(std::ostream &os) {
		os.write(data, getBytes() + getMessageOverhead());
		return os;
	}
};

int main(int argc, char *argv[]) {

	int num = 127;

	vector< marked_istream_ptr > is(num, marked_istream_ptr());
	vector< marked_ostream_ptr > os(num, marked_ostream_ptr());
	vector< float > mbps(omp_get_max_threads(), 0);

	int cycles = 1000;
	int burstMean = 32, burstStd;
	int waitMicroMean = 0, waitMicroStd;
	int bufferSize = 8192, numBuffers = 256;
	if (argc >= 2) {
		cycles = atoi(argv[1]);
	}
	if (argc >= 3) {
		burstMean = atoi(argv[2]);
	}
	burstStd = burstMean * 2;
	if (argc >= 4) {
		waitMicroMean = atoi(argv[3]);
	}
	waitMicroStd = waitMicroMean * 2;
	if (argc >= 5) {
		bufferSize = atoi(argv[4]);
	}
	if (argc >= 6) {
		numBuffers = atoi(argv[5]);
	}
	LOG("cycles: " << cycles << ", avgMessageBytes: " << burstMean << ", avgMessageDelay: " << waitMicroMean << " us, bufferSize: " << bufferSize << ", numBuffers: " << numBuffers);

	int activeWriters, readers, writers;

	for (readers = 1 ; readers < omp_get_max_threads(); readers++) {
		LOG("Running with " << readers << " readers, " << omp_get_max_threads()-readers << " writers");
		boost::system_time start = boost::get_system_time();

		BufferFifo bfifo(bufferSize, numBuffers);
		int inMessages = 0, outMessages = 0;

#pragma omp parallel for
		for(int i = 0; i < num ; i++) {
			is[i].reset( new marked_istream(bfifo) );
			os[i].reset( new marked_ostream(bfifo) );
		}

		// test many outputs, one input
#pragma omp parallel
		{
			int threadId = omp_get_thread_num();
			int numThreads = omp_get_num_threads();
			long myBytes = 0;
			int myMessages = 0;

			boost::random::mt19937 rng; rng.seed( threadId * threadId * threadId * threadId );
			boost::random::normal_distribution<> burst_bytes(burstMean, burstStd), wait_us(waitMicroMean, waitMicroStd);
#pragma omp single
			{
				writers = numThreads-readers;
				activeWriters = writers;
			}
			boost::system_time myStart = boost::get_system_time();

			//std::cout << "Starting thread " << threadId << std::endl;
			if (threadId < readers) {
				MessageTest msg;
				// scan through all os until no more writers
				int lastpass = 1;
				while(lastpass) {
					if (bfifo.isEOF())
						lastpass--;
					for(int i = 0; i < num ; i++) {
						if ((i % readers) != threadId)
							continue;
						int messages = 0, totalBytes = 0;
						assert(is[i]->good());
						while (is[i]->isReady()) {
							msg.read(*is[i]);
							totalBytes += msg.getBytes();
							myBytes += msg.getBytes();
							assert(msg.validate());
							messages++;
							assert(is[i]->good());
						}
						myMessages += messages;
					}
				}
				for(int i = 0; i < num ; i++) {
					if ((i % readers) != threadId)
						continue;
					is[i].reset();
				}

#pragma omp atomic
				inMessages += myMessages;

				//LOG("Input Thread Finished: " << myMessages << " messages");
			} // reader
			else { // writer

				MessageTest msg;
				for(int j = 0; j < cycles ; j++) {
					for(int i = 0; i < num; i++) {
						if ((i % writers) + readers != threadId)
							continue;
						assert(os[i]->good());
						int blockBytes;
						while ((blockBytes = burst_bytes(rng)) <= 0);
						msg.setMessage(i, blockBytes);
						assert(msg.validate());
						msg.write(*os[i]);
						os[i]->setMark();
						assert(os[i]->good());
						myMessages++;
						myBytes += blockBytes;
						long waittime;
						while ((waittime = (waitMicroMean > 0 ? wait_us(rng) : 0)) < 0);
						boost::this_thread::sleep( boost::posix_time::microseconds( waittime ) );
					}
				}

				// Finish up.
				for(int i = 0; i < num; i++) {
					if ((i % writers) + readers != threadId)
						continue;
					os[i]->flush();
					os[i].reset();
				}

#pragma omp atomic
				outMessages += myMessages;

				//LOG("Output Thread Finished: " << myMessages);

#pragma omp critical
				{
					// only the last one should setEOF
					if (--activeWriters == 0 && bfifo.getActiveWriterCount() == 0) {
						bfifo.setEOF();
					}
				}
			} // writer
			boost::system_time myEnd = boost::get_system_time();
			mbps[ threadId ] = (myBytes / 1000000.0) / ((myEnd - myStart).total_microseconds() / 1000000.0);
		}  // parallel

		boost::system_time end = boost::get_system_time();
		std::stringstream ss;
		for(int i = 0; i < (int) mbps.size(); i++)
			ss << ", " << mbps[i];
		std::string str = ss.str();

		LOG("Wrote " << outMessages << " Read " << inMessages << ". " << (end - start).total_milliseconds() << "ms " << str);
		LOG(bfifo.getState());
		assert(outMessages == inMessages);
	} // number of readers


	return 0;
}
