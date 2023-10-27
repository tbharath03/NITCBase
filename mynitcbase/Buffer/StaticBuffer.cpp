#include "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];

StaticBuffer::StaticBuffer() {
	// copy blockAllocMap blocks from disk to buffer (using readblock() of disk)
  // blocks 0 to 3
  for(int i=0;i<4;i++){
  	unsigned char *temp=(blockAllocMap)+(i*2048);
  	//Disk::readBlock(*(blockAllocMap)+(i*2048),i);
  	Disk::readBlock(temp,i);
  }

  // initialise all blocks as free
  for (int BufferIndex=0;BufferIndex<BUFFER_CAPACITY;BufferIndex++) {
    metainfo[BufferIndex].free = true;
    metainfo[BufferIndex].dirty=false;
    metainfo[BufferIndex].timeStamp=-1;
    metainfo[BufferIndex].blockNum=-1;
  }
}
/*
At this stage, we are not writing back from the buffer to the disk since we are
not modifying the buffer. So, we will define an empty destructor for now. In
subsequent stages, we will implement the write-back functionality here.
*/
StaticBuffer::~StaticBuffer() {

	for(int i=0;i<4;i++){
		unsigned char *temp=(blockAllocMap)+(i*2048);
  		//Disk::writeBlock(*(blockAllocMap)+(i*2048),i);
  		Disk::writeBlock(temp,i);
  	}

	for(int i=0;i<BUFFER_CAPACITY;i++){
	/*iterate through all the buffer blocks,
    write back blocks with metainfo as free=false,dirty=true
    using Disk::writeBlock()
    */
		if(metainfo[i].free==false && metainfo[i].dirty==true){
			Disk::writeBlock(blocks[i],metainfo[i].blockNum);
		}
	}
}

int StaticBuffer::getFreeBuffer(int blockNum){
    // Check if blockNum is valid (non zero and less than DISK_BLOCKS)
    // and return E_OUTOFBOUND if not valid.
    if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    	return E_OUTOFBOUND;
  	}	

    // increase the timeStamp in metaInfo of all occupied buffers.

    for(int i=0;i<BUFFER_CAPACITY;i++){
		if(metainfo[i].free==false){
			metainfo[i].timeStamp++;
		}  
  	}

    // let bufferNum be used to store the buffer number of the free/freed buffer.
    int bufferNum=-1;

    // iterate through metainfo and check if there is any buffer free

    // if a free buffer is available, set bufferNum = index of that free buffer.
    for (int BufferIndex=0;BufferIndex<BUFFER_CAPACITY;BufferIndex++) {
    	if(metainfo[BufferIndex].free == true){
    		bufferNum=BufferIndex;
    		break;
    	}
  	}

    // if a free buffer is not available,
    //     find the buffer with the largest timestamp
    //     IF IT IS DIRTY, write back to the disk using Disk::writeBlock()
    //     set bufferNum = index of this buffer

    if(bufferNum==-1){
    	int maxi=-1e9;
    	int buffNum;
    	for(int i=0;i<BUFFER_CAPACITY;i++){
    		if(metainfo[i].timeStamp>maxi){
    			maxi=metainfo[i].timeStamp;
    			buffNum=i;
    		}
    	}
    	if(metainfo[buffNum].dirty){
    		Disk::writeBlock(blocks[buffNum],metainfo[buffNum].blockNum);
    	}
    	bufferNum=buffNum;
    }
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////    
    
    
    // update the metaInfo entry corresponding to bufferNum with
    // free:false, dirty:false, blockNum:the input block number, timeStamp:0.
    metainfo[bufferNum].free = false;
    metainfo[bufferNum].dirty=false;
  	metainfo[bufferNum].blockNum = blockNum;

    // return the bufferNum.
    return bufferNum;
}

/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum) {
  // Check if blockNum is valid (between zero and DISK_BLOCKS)
  // and return E_OUTOFBOUND if not valid.
  if (blockNum < 0 || blockNum > DISK_BLOCKS) {
    return E_OUTOFBOUND;
  }

  // find and return the bufferIndex which corresponds to blockNum (check metainfo)
  for (int BufferIndex=0;BufferIndex<BUFFER_CAPACITY;BufferIndex++) {
    if(metainfo[BufferIndex].blockNum==blockNum){
    	return BufferIndex;
    }
  }

  // if block is not in the buffer
  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum){
    // find the buffer index corresponding to the block using getBufferNum().
    int bufferNum=getBufferNum(blockNum);

    // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
    //     return E_BLOCKNOTINBUFFER
    if(bufferNum==E_BLOCKNOTINBUFFER){
    	return E_BLOCKNOTINBUFFER;
    }

    // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
    //     return E_OUTOFBOUND
	if(bufferNum==E_OUTOFBOUND){
		return E_OUTOFBOUND;
	}
	else{
		metainfo[bufferNum].dirty=true;
	}
	
    // else
    //     (the bufferNum is valid)
    //     set the dirty bit of that buffer to true in metainfo

    // return SUCCESS
    return SUCCESS;
}

int StaticBuffer::getStaticBlockType(int blockNum){
    // Check if blockNum is valid (non zero and less than number of disk blocks)
    // and return E_OUTOFBOUND if not valid.
    if(blockNum<0 || blockNum>=DISK_BLOCKS){
      return E_OUTOFBOUND;
    }

    // Access the entry in block allocation map corresponding to the blockNum argument
    // and return the block type after type casting to integer.
    return (int)blockAllocMap[blockNum];
}