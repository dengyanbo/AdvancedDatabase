#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"

/* manipulating page files */
//***********************************YH***************************************
void initStorageManager (void){}
RC createPageFile(char *fileName)
{
    //open a file for writing and reading 
    FILE *fp = fopen(fileName, "w+");

    //disable the buffer so that following process are executed directly to disk
    setbuf(fp, NULL);

    //check if the file exists
    if(fp == NULL){
        return RC_FILE_NOT_FOUND;
    }

    //create a string with size of block_size and fill out the memory with 0s
    SM_PageHandle ph = (SM_PageHandle) malloc(PAGE_SIZE);
    memset(ph, '\0', PAGE_SIZE);
   
    //assign the block to file 
    long file_size=fwrite(ph, sizeof(char), PAGE_SIZE, fp); 
    
    //check if the file has enough space to write the page
    if (PAGE_SIZE != file_size) {
        return RC_WRITE_FAILED;
    }
    
    //close the file and free cache
    fclose(fp);	
    free(ph);  
    
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    //open a file for reading and writing and check if file exists
    FILE *fp = fopen(fileName, "r+");
    if(fp == NULL){
        return RC_FILE_NOT_FOUND;
    }

    //move pointer to end of the file
    long reposit=fseek(fp, 0, SEEK_END);
    if(reposit == -1){
        return RC_READ_NON_EXISTING_PAGE;
}
    
    //add 1 to the file size to make sure below total number of pages is correct
    long fileSize = ftell(fp)+1;
    
    //assign fhandle variables
    fHandle->fileName = fileName;
    fHandle->totalNumPages = fileSize/PAGE_SIZE;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = fp;
    
    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle)
{
    //check if file exists, if yes close it
    if(fclose(fHandle->mgmtInfo) == -1){
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}

RC destroyPageFile(char *fileName)
{
    //check if file exists, if yes destroy it
    if(remove(fileName) == -1) {
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}
//****************************************************************************

//****************************************************************************

/* reading blocks from disc */
//**********************************ZZ***************************************
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	// check if the file is opened or not
	if (fHandle->mgmtInfo == NULL)
	{
		return RC_FILE_NOT_FOUND;
	}
	//check if the current page number exceed the total pages
	if (fHandle->totalNumPages <= pageNum)
	{
		return RC_READ_NON_EXISTING_PAGE;
	}
	//set the pointer to the current page position
	fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET);
	//read page file to current block
	if (fread(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo) != 1)
	{
		printf("Read ERROR,cannot read page to file");
		return RC_READ_NON_EXISTING_PAGE;
	}
	return RC_OK;
}


int getBlockPos(SM_FileHandle *fHandle)
{
	if (fHandle->mgmtInfo == NULL)
	{
		return RC_FILE_NOT_FOUND;
	}
	return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	// check if the current page is the first page 
	if (fHandle->curPagePos <= 0 || fHandle->totalNumPages < fHandle->curPagePos)
	{
		return RC_READ_NON_EXISTING_PAGE;
	}
	return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}
//****************************************************************************

//************************************XM**************************************
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (fHandle->mgmtInfo == NULL)
	{
		return RC_FILE_NOT_FOUND;
	}
	//the currnt block postion should be start from current page
	return readBlock(fHandle->curPagePos, fHandle, memPage);//
}


RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//if the NextBlock is beyond the last pagefile
	//then it should return RC_READ_NON_EXISTING_PAGE
	if (fHandle->curPagePos >= fHandle->totalNumPages - 1)
	{
		return RC_READ_NON_EXISTING_PAGE;
	}
	else
	{
		//else, it should return the current page position +1
		return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
	}
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//the last page of blkock should be equals to the totalNumPages-1
	return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}
//*****************************************************************************

/* writing blocks to a page file */
//************************************YD***************************************
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    // check if mgmtInfo point to the correct file header
    if(fHandle->mgmtInfo == NULL) {
        printf("Write Error! File header not found!\n");
        return RC_FILE_HANDLE_NOT_INIT;
    }
    // check if pageNum exists in the fHandle
    while(fHandle->totalNumPages <= pageNum) {
        appendEmptyBlock(fHandle);
        printf("auto add one empty page\n");
    }
    // set the file header to the current block (pageNum)
    printf("size: %lu, pageNum*Page_Size = %d\n", sizeof(fHandle->mgmtInfo), pageNum*PAGE_SIZE);
    fseek(fHandle->mgmtInfo, pageNum * PAGE_SIZE, SEEK_SET);
    // write page file (memPage) to current block
    if(fwrite(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo) != 1){
        printf("Write Error! Cannot write page to file!\n");
        return RC_WRITE_FAILED;
    }
    return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // get current block position
    int pos = getBlockPos(fHandle);
    // check fHandle has correct file header
    return writeBlock(pos, fHandle, memPage);
}

RC appendEmptyBlock (SM_FileHandle *fHandle){
    // check fHandle has correct file header
    if(fHandle->mgmtInfo == NULL) {
        printf("Appending Error! File header not found!\n");
        return RC_FILE_HANDLE_NOT_INIT;
    }
    // Initialize a page handle, allocate PAGE_SIZE memory and fill it with '\0'
    SM_PageHandle emptyPh;
    if((emptyPh = (SM_PageHandle) malloc(PAGE_SIZE)) == NULL){
        printf("Appending Error! Cannot allocate memory for one page!\n");
        return RC_WRITE_FAILED;
    }
    memset(emptyPh, '\0', PAGE_SIZE);

    fHandle->totalNumPages +=  1;
    // set the file header to the start of the empty block
    fseek(fHandle->mgmtInfo, 1, SEEK_END);
    // write the empty page to the file
    fprintf(fHandle->mgmtInfo, "%s", emptyPh);
    // free the memory of pageHandle
    free(emptyPh);
    fHandle->curPagePos = fHandle->totalNumPages - 1;
    return RC_OK;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    // check fHandle has correct file header
    if(fHandle->mgmtInfo == NULL){
        printf("Capacity Error! File header not found!\n");
        return RC_FILE_HANDLE_NOT_INIT;
    }
    // check if the capacity of the file is big enough
    if(numberOfPages <= fHandle->totalNumPages){
        printf("Capacicty is enough.\n");
        return RC_OK;
    }
    // calculate how many empty pages we need to append to the end of the file
    int numAppendingPage = numberOfPages - fHandle->totalNumPages;
    // append # of numAppendingPage empty pages to the end of the file
    for(int i = 0; i < numAppendingPage; ++i){
        int RC_RETURNED = appendEmptyBlock(fHandle);
        if(RC_RETURNED == RC_OK) continue;
        return RC_RETURNED;
    }
    return RC_OK;
}
//*****************************************************************************