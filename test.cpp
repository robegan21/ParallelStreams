// module load boost/1.53.0
// g++ -Wall -g -gstabs -fopenmp -I $BOOST_DIR/include -L $BOOST_DIR/lib test.cpp -lboost_system -lboost_thread


#include "Buffer.hpp"
#include "marked_iostream.hpp"

#include "stdio.h"
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_smallint.hpp>
#include <boost/random/normal_distribution.hpp>

using namespace std;

typedef boost::shared_ptr< marked_istream > marked_istream_ptr;
typedef boost::shared_ptr< marked_ostream > marked_ostream_ptr;


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
		metaData.write(os);
		os.write(data, metaData.getBytes());
		//LOG("Message::write " << metaData.getBytes() << " os: " << (long) &os);
		assert(os.good());
		return os;
	}
	void reserve(int32_t n) {
		data = (char*) realloc(data, n);
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

class MessageTest : public BaseMessage<BlockId> {
public:
	void setMessage(int32_t id, int32_t size) {
		reserve(size);
		char c = (char) id;
		metaData.setBytes(size);
		metaData.blockId = id;
		for(int32_t i = 0; i < size; i++)
			data[i] = c;
	}
	bool validate() const {
		bool valid = true;
		for(int32_t i = 0; i < metaData.getBytes(); i++) {
			valid |= ((int) data[i]) == metaData.blockId;
		}
		//printf("MessageTest::validate(): T%d Message from %d with %d bytes\n", omp_get_thread_num(), metaData.blockId, metaData.getBytes());
		//std::cout << "Message " << metaData.getBytes() << " from " << metaData.blockId << std::endl;
		return valid;
	}
};

int main(int argc, char *argv[]) {
	{
	BufferFifo bfifo;
	int num = 127;

	vector< marked_istream_ptr > is(num, marked_istream_ptr());
	vector< marked_ostream_ptr > os(num, marked_ostream_ptr());

	int burstMean = 500, burstStd = 100, waitMicroMean = 2000, waitMicroStd = 20000;
	int cycles = 1000;
	int inMessages = 0, outMessages = 0;
	int activeWriters, readers, writers;

	for (readers = 1 ; readers < omp_get_max_threads()-1; readers++) {
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

		writers = numThreads-readers;
		activeWriters = writers;

#pragma omp barrier

		boost::random::mt19937 rng; rng.seed( threadId * threadId * threadId * threadId );
		boost::random::normal_distribution<> burst_bytes(burstMean, burstStd), wait_us(waitMicroMean, waitMicroStd);

		//std::cout << "Starting thread " << threadId << std::endl;
		if (threadId < readers) {
			MessageTest msg;
			// scan through all os until no more writers
			bool lastpass = false;
			while(!lastpass) {
				if (bfifo.isEOF())
					lastpass = true;
				for(int i = 0; i < num ; i++) {
					if ((i % readers) != threadId)
						continue;
					int messages = 0, totalBytes;
					//if (!is[i]->good()) {
						//std::cerr << "is " << i << " is not good!" << std::endl;
						//lastpass = true;
					//}
					while (is[i]->isReady()) {
						msg.read(*is[i]);
						totalBytes += msg.getBytes();
						assert(msg.validate());
						messages++;
					}
					if (messages == 0) {
						is[i]->sync();
					}
#pragma omp atomic
					inMessages += messages;
					//std::cerr << threadId << ": read " << messages << " messages " << totalBytes << " bytes." << std::endl;

					//std::cerr << "writers: " << writers << std::endl;
				}
			}
			LOG("Input Thread Finished");
		} else {


			MessageTest msg;
			for(int j = 0; j < cycles ; j++) {
				for(int i = 0; i < num; i++) {
					if ((i % writers) + readers != threadId)
						continue;
					//if (!os[i]->good()) {
					//	std::cerr << "os " << i << " is not good!" << std::endl;
					//	break;
					//}
					int blockBytes = burst_bytes(rng);
					msg.setMessage(i, blockBytes);
					assert(msg.validate());
					//LOG("Writing " << i << " msg " << blockBytes << " to " << (long) os[i].get());
					msg.write(*os[i]);
					//LOG("Setting mark " << i << " msg " << blockBytes << " to " << (long) os[i].get());
					os[i]->setMark();
#pragma omp atomic
					outMessages++;
					//std::cerr << "Thread " << threadId << " wrote " << bytes << " to " << i << ". " << os[i]->tellp() << std::endl;
				}
			}

			// Finish up.
			for(int i = 0; i < num; i++) {
				if ((i % writers) + readers != threadId)
					continue;
				os[i]->flush();
				os[i].reset();
			}

			LOG(" Output Thread Finished");

#pragma omp critical
			{
				// only the last one should setEOF
				if (--activeWriters == 0 && bfifo.getActiveWriterCount() == 0) {
					bfifo.setEOF();
					LOG("Wrote " << outMessages << " messages");
				}
			}
		}
	}}
	}

	return 0;
}
