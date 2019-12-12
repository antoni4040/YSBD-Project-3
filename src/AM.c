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

int recursiveInsert(int currentBlock, void *value1, void *value2) {

}

int createIndexBlock(int fd, void* key, int block) {
  //Initializations.
  BF_Block *indexBlock;
  BF_Block_Init(&indexBlock);
  char* recordData;
  CALL_BF(BF_AllocateBlock(fd, indexBlock));
  recordData = BF_Block_GetData(indexBlock);

  //Add "I" character in the first byte.

  //Add counter set to 1. 
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
  openAM* currentAM = tableOfIndexes[fileDesc];
  int fd = currentAM->fd;

  //If empty, create root and one leaf.
  if(currentAM->root == 0) {

  }
  //Else, go through B+ tree.
  else {

  }

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
