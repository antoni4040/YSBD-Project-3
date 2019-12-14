#include "AM.h"

#include "bf.h" //HOW TF DO YOU FORGET THAT?
#include "defn.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAX_OPEN_FILES 20

#define BLOCK_METADATA (sizeof(char) + 2*sizeof(int))

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return AME_EOF;        \
  }                         \
}

int AM_errno = AME_OK;

typedef struct {
  char attrType1; 
  int attrLength1;
  char attrType2;
  int attrLength2;
  int root;
  int fd;
  int dataSize;
  int indexSize;
} openAM;

openAM* tableOfIndexes[MAX_OPEN_FILES]; 
int openFiles = 0;


// Calculate how many elements fit in a data(leaf) block.
int howManyInDataBlock(openAM* currentAM) {
  int keyPlusDataSize = currentAM->attrLength1 + currentAM->attrLength2;
  int count = (BF_BLOCK_SIZE - BLOCK_METADATA) / keyPlusDataSize;
  return count;
}


// Calculate how many elements fit in an index block.
int howManyInIndexBlock(openAM* currentAM) {
  int keyPlusPointerSize = currentAM->attrLength1 + sizeof(int);
  int count = (BF_BLOCK_SIZE - BLOCK_METADATA) / keyPlusPointerSize;
  return count;
}


int keyCompare(openAM* bplus, void* value1, void* keyValue) {
  if(bplus->attrType1 == 'i') {
    int value1Int;
    memcpy(&value1Int, value1, sizeof(int));
    return value1Int - *((int*)keyValue);
  }
  else if(bplus->attrType1 == 'f') {
    float value1Float;
    memcpy(&value1Float, value1, sizeof(float));
    return value1Float > *((float*)keyValue) ? 1 : (
        value1Float < *((float*)keyValue) ? -1 : 0);
  }
  else {
    return strncmp((char*)value1, (char*)keyValue, bplus->attrLength1);
  }
}


int recursiveInsert(openAM* bplus, int currentBlockNum, void *value1, void *value2, void* returnKey) {
  //Get current block.
  BF_Block *currentBlock;
  BF_Block_Init(&currentBlock);
  CALL_BF(BF_GetBlock(bplus->fd, currentBlockNum, currentBlock));
  char* data = BF_Block_GetData(currentBlock);
  char* dataPoint = data;
  int sizeOfIndexElements = bplus->attrLength1 + sizeof(int);
  int sizeOfDataElements = bplus->attrLength1 + bplus->attrLength2;
  int count;

  //If index block:
  if(data[0] == 'I') {
    dataPoint += sizeof(char);
    memcpy(&count, dataPoint, sizeof(int));
    dataPoint += sizeof(int);
    int visited = 0;
    int nextBlockNum;
    int insertResult;
    //Go to each element and if keyCompare returns negative, go to left pointer.
    for(int i = 0; i < count; i++) {
      if(keyCompare(bplus, value1, (void*)(dataPoint+sizeof(int))) < 0) {
        memcpy(&nextBlockNum, dataPoint, sizeof(int)); //Left pointer.
        insertResult = recursiveInsert(bplus, nextBlockNum, value1, value2, returnKey);
        visited = 1;
      }
      dataPoint += sizeOfIndexElements;
    }
    //If no pointer visited, go to last pointer. 
    if(visited == 0) {
      memcpy(&nextBlockNum, dataPoint, sizeof(int)); //Last pointer.
      insertResult = recursiveInsert(bplus, nextBlockNum, value1, value2, returnKey);
    }

    //Split happened.
    if(insertResult != 0) {

    }
    else 
      return 0;
  }
  //If leaf(data) block:
  else {
    //Find spot to put new item.
    dataPoint += sizeof(char);
    memcpy(&count, dataPoint, sizeof(int));
    dataPoint += sizeof(int);
    int insertionPoint = -1;
    for(int i = 0; i < count; i++) { 
      if(keyCompare(bplus, value1, (void*)(dataPoint)) < 0) {
        insertionPoint = i;
        break;
      }
      dataPoint += sizeOfDataElements;
    }
    //If split necessary.
    if(bplus->dataSize == count) {
      //Make buffer for data cause we lazy(but awesome).
      char* buffer = malloc(sizeOfDataElements*(bplus->dataSize + 1));
      char* bufferPoint = buffer;
      //Copy all elements to buffer and add new one at the end.
      if(insertionPoint == -1) {
        memcpy(bufferPoint, data + sizeof(char) + sizeof(int), sizeOfDataElements*count);
        bufferPoint += sizeOfDataElements*count;
        memcpy(bufferPoint, value1, bplus->attrLength1);
        bufferPoint += bplus->attrLength1;
        memcpy(bufferPoint, value2, bplus->attrLength2);
      }
      else {
        memcpy(bufferPoint, data + sizeof(char) + sizeof(int), sizeOfDataElements*insertionPoint);
        bufferPoint += sizeOfDataElements*insertionPoint;
        memcpy(bufferPoint, value1, bplus->attrLength1);
        bufferPoint += bplus->attrLength1;
        memcpy(bufferPoint, value2, bplus->attrLength2);
        bufferPoint += bplus->attrLength2;
        char* restData = data + sizeof(char) + sizeof(int) + sizeOfDataElements*insertionPoint;
        memcpy(bufferPoint, restData, sizeOfDataElements*(count - insertionPoint));
      }

      int halfPoint = (count + 1) / 2;

      //Copy first half of buffer to existing block.
      memcpy(data+sizeof(char)+sizeof(int), buffer, halfPoint*sizeOfDataElements);
      //Change original count.
      memcpy(data+sizeof(char), &halfPoint, sizeof(int));

      //Copy second half of buffer to new block.
      BF_Block *newBlock;
      BF_Block_Init(&newBlock);
      CALL_BF(BF_AllocateBlock(bplus->fd, newBlock));
      char* newData = BF_Block_GetData(newBlock);

      //Setup new data block.

      free(buffer);
    }
    else {
      //Insert at last position.
      if(insertionPoint == -1) {
        memcpy(dataPoint, value1, bplus->attrLength1);
        dataPoint += bplus->attrLength1;
        memcpy(dataPoint, value2, bplus->attrLength2);
        int newCount = count + 1;
        memcpy(data+sizeof(char), &newCount, sizeof(int));
      }
      //Insert between elements.
      else {
        memmove(dataPoint+sizeOfDataElements, dataPoint,
                (count - insertionPoint)*sizeOfDataElements);
        memcpy(dataPoint, value1, bplus->attrLength1);
        dataPoint += bplus->attrLength1;
        memcpy(dataPoint, value2, bplus->attrLength2);
        int newCount = count + 1;
        memcpy(data+sizeof(char), &newCount, sizeof(int));
      }
    }
  }
}


void AM_Init() {
  for(int i = 0; i < MAX_OPEN_FILES; i++) {
      tableOfIndexes[i] = NULL;
  }
  BF_Init(LRU);
	return;
}


int AM_CreateIndex(char *fileName, char attrType1, 
	                int attrLength1, char attrType2,
                  int attrLength2) {
  //Checking if the attribute values are valid.
  if(attrType1 == 'c'){
    if(attrLength1 > 255 || attrLength1 < 1){
      AM_errno = AME_PARAMETER;
      return AM_errno; 
    }
  }
  else if(attrType1 == 'i'){
    if(attrLength1 != sizeof(int)){
      AM_errno = AME_PARAMETER;
      return AM_errno;
    }
  }
  else if(attrType1 == 'f'){
    if(attrLength1 != sizeof(float)) {
      AM_errno = AME_PARAMETER;
      return AM_errno;
    }
  }
  else {
    AM_errno = AME_PARAMETER;
    return AM_errno;
  }

  if(attrType2 == 'c'){
    if(attrLength2 > 255 || attrLength2 < 1){
      AM_errno = AME_PARAMETER;
      return AM_errno; 
    }
  }
  else if(attrType2 == 'i'){
    if(attrLength2 != sizeof(int)){
      AM_errno = AME_PARAMETER;
      return AM_errno;
    }
  }
  else if(attrType2 == 'f' ){
    if(attrLength2 != sizeof(float)){
      AM_errno = AME_PARAMETER;
      return AM_errno;
    }
  }
  else {
    AM_errno = AME_PARAMETER;
      return AM_errno;
  }                   

  
  // File must not already exist.
	if(access(fileName, F_OK) == 0){
		printf("File already exists. \n");
    AM_errno = AME_EXISTS;
    return AM_errno;
	}
  
  int fd;
  CALL_BF(BF_CreateFile(fileName));
  CALL_BF(BF_OpenFile(fileName, &fd));

  // Create first block.
  BF_Block *block;
  char* data;
  BF_Block_Init(&block);
  CALL_BF(BF_AllocateBlock(fd, block));
  data = BF_Block_GetData(block);

  // Write "BP" in the beginning of first block to signify
  // we have an B+ tree file.
  char str1[3];
  strcpy(str1, "BP");
  memcpy(data, str1, 2);

  //If root == 0, there is no root yet.
  int root = 0;

  // Write attributes.
  char* dataPoint = data+2;
  memcpy(dataPoint, &attrType1, sizeof(char));
  dataPoint += sizeof(char);
  memcpy(dataPoint, &attrLength1, sizeof(int));
  dataPoint += sizeof(int);
  memcpy(dataPoint, &attrType2, sizeof(char));
  dataPoint += sizeof(char);
  memcpy(dataPoint, &attrLength2, sizeof(int));
  dataPoint += sizeof(int);
  memcpy(dataPoint, &root, sizeof(int));

  // Save changes to first block.
  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  // Make changes and close HT file.
  BF_Block_Destroy(&block);
  CALL_BF(BF_CloseFile(fd));

  return AME_OK;
}


int AM_DestroyIndex(char *fileName) {
  int status = remove(fileName);
  return AME_OK;
}


int AM_OpenIndex(char *fileName) {
  char attrType1;
	int attrLength1;
  char attrType2;
  int attrLength2;
  int root;

  // If we have reached max open files, return error.
  if(openFiles == MAX_OPEN_FILES) {
    fprintf(stderr, "Can't open more files.\n");
    AM_errno = AME_MAX_FILES;
    return AM_errno;
  }

  openFiles++;

  // Open file.
  int fd;
  CALL_BF(BF_OpenFile(fileName, &fd));

  // Init block.
  BF_Block *block;
  BF_Block_Init(&block);
  char* data;
  CALL_BF(BF_GetBlock(fd, 0, block));
  data = BF_Block_GetData(block);

  // Check if it's not B+ file and close it.
  if(strncmp(data, "BP", 2) != 0) {
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    CALL_BF(BF_CloseFile(fd));
    AM_errno = AME_NOT_BP;
    return AM_errno;
  }

  // Read attributes.
  char* dataPoint = data+2;
  memcpy(&attrType1, dataPoint, sizeof(char));
  dataPoint += sizeof(char);
  memcpy(&attrLength1, dataPoint, sizeof(int));
  dataPoint += sizeof(int);
  memcpy(&attrType2, dataPoint, sizeof(char));
  dataPoint += sizeof(char);
  memcpy(&attrLength2, dataPoint, sizeof(int));
  dataPoint += sizeof(int);
  memcpy(&root, dataPoint, sizeof(int));
  

  //Find first empty position in table of indexes and put the file descriptor.
  int index = 0;
  while(index < MAX_OPEN_FILES) {
    if(tableOfIndexes[index] == NULL) {
      tableOfIndexes[index] = malloc(sizeof(openAM));
      tableOfIndexes[index]->fd = fd;
      tableOfIndexes[index]->attrType1 = attrType1;
      tableOfIndexes[index]->attrLength1 = attrLength1;
      tableOfIndexes[index]->attrType2 = attrType2;
      tableOfIndexes[index]->attrLength2 = attrLength2;
      tableOfIndexes[index]->root = root;
      tableOfIndexes[index]->indexSize = howManyInIndexBlock(tableOfIndexes[index]);
      tableOfIndexes[index]->dataSize = howManyInDataBlock(tableOfIndexes[index]);
      break;
    }
    index++;
  }
  
  // Close block.
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);

  return index;
}


int AM_CloseIndex(int fileDesc) {
  openAM* indexToDie = tableOfIndexes[fileDesc];
  BF_CloseFile(indexToDie->fd);
  free(indexToDie);
  tableOfIndexes[fileDesc] = NULL;
  openFiles--;
  return AME_OK;
}


int AM_InsertEntry(int fileDesc, void *value1, void *value2) {
  openAM* bplus = tableOfIndexes[fileDesc];
  int fd = bplus->fd;

  //If empty, create root and one leaf.
  if(bplus->root == 0) {
    BF_Block *dataBlock;
    BF_Block_Init(&dataBlock);
    char* recordData;
    CALL_BF(BF_AllocateBlock(bplus->fd, dataBlock));
    recordData = BF_Block_GetData(dataBlock);

    recordData[0] = 'D';
    char* recordDataPoint = recordData + sizeof(char);

    int count = 1;
    memcpy(recordDataPoint, &count, sizeof(int));

    recordDataPoint += sizeof(int);
    memcpy(recordDataPoint, value1, bplus->attrLength1);
    recordDataPoint += bplus->attrLength1;
    memcpy(recordDataPoint, value2, bplus->attrLength2);

    int next = -1;
    memcpy(recordData + BF_BLOCK_SIZE - sizeof(int), &next, sizeof(int));

    BF_Block_SetDirty(dataBlock);
    CALL_BF(BF_UnpinBlock(dataBlock));
    BF_Block_Destroy(&dataBlock);

    bplus->root = 1;
  }
  //Else, go through B+ tree.
  else {
    void* returnKey = NULL;
    recursiveInsert(bplus, bplus->root, value1, value2, returnKey);
    if(returnKey != NULL) {
      free(returnKey);
    }
  }
  return AME_OK;
}

int AM_printDataBlock(int fileDesc, int blockNum) {
  openAM* bplus = tableOfIndexes[fileDesc];
  int fd = bplus->fd;

  BF_Block *dataBlock;
  BF_Block_Init(&dataBlock);
  CALL_BF(BF_GetBlock(fd, blockNum, dataBlock));
  char* recordData = BF_Block_GetData(dataBlock);

  int count;
  memcpy(&count, recordData+sizeof(char), sizeof(int));
  char* recordDataPoint = recordData + sizeof(char) + sizeof(int);
  for(int i = 0; i < count; i++) {
    if(bplus->attrType1 == 'i') {
      int key;
      memcpy(&key, recordDataPoint, sizeof(int));
      printf("Key: %d\n", key);
    }
    else if(bplus->attrType1 == 'f') {
      float key;
      memcpy(&key, recordDataPoint, sizeof(float));
      printf("Key: %f\n", key);
    }
    else {
      char key[255];
      memcpy(&key, recordDataPoint, bplus->attrLength1);
      printf("Key: %s\n", key);
    }

    recordDataPoint += bplus->attrLength1;

    if(bplus->attrType2 == 'i') {
      int value;
      memcpy(&value, recordDataPoint, sizeof(int));
      printf("Value: %d\n", value);
    }
    else if(bplus->attrType2 == 'f') {
      float value;
      memcpy(&value, recordDataPoint, sizeof(float));
      printf("Value: %f\n", value);
    }
    else {
      char value[255];
      memcpy(&value, recordDataPoint, bplus->attrLength2);
      printf("Value: %s\n", value);
    }

    recordDataPoint += bplus->attrLength2;
  }

  CALL_BF(BF_UnpinBlock(dataBlock));
  BF_Block_Destroy(&dataBlock);

  return AME_OK;
}


int AM_OpenIndexScan(int fileDesc, int op, void *value) {
  return AME_OK;
}


void *AM_FindNextEntry(int scanDesc) {
	
}


int AM_CloseIndexScan(int scanDesc) {
  return AME_OK;
}


void AM_PrintError(char *errString) {
  
}

void AM_Close() {
  BF_Close();
}
