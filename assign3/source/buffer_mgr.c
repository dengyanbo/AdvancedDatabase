#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"

#define NUMPAGES 1000 //define total number of file pages


Frame *newFrame(int frameIndex){
    Frame *fr = malloc(sizeof(Frame));
    fr->previous = NULL;
    fr->next = NULL;
    fr->ph = malloc(sizeof(BM_PageHandle));
    fr->ph->data = (char*)malloc(sizeof(char)*PAGE_SIZE);
    fr->fixCount = 0;
    fr->dirty=0;
    return fr;
}

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData){
    BM_DataManager *dm = malloc(sizeof(BM_DataManager)); 
    dm->fhead = dm->ftail = newFrame(0);
    dm->fempty= dm->fhead; 
    bm->pageFile = (char*)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;   
    SM_FileHandle fh;  
    openPageFile((char*)pageFileName, &fh);   

    int i=1;
    while(i<numPages){
	dm->ftail->next = newFrame(i);
       	dm->ftail->next->previous = dm->ftail;
       	dm->ftail = dm->ftail->next;
	i=i+1;
	}
	
    memset(dm->copyPage, 0, sizeof(Frame*)*NUMPAGES);
    while(i<NUMPAGES){
	dm->copyPage[i]=(Frame*)NULL;
	i=i+1;
	}	
    bm->mgmtData = dm;

    closePageFile(&fh);
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
    BM_DataManager *dm = bm->mgmtData;
    Frame *current = dm->fhead;
    forceFlushPool(bm);
    current = dm->fhead;
    current=current->next;
    while(current->next!=NULL){
		free(dm->fhead->ph);
		free(dm->fhead);
		dm->fhead = current;
current = current->next;
	}

    free(dm);
    return RC_OK;

}

RC forceFlushPool(BM_BufferPool *const bm){
    BM_DataManager *dm = bm->mgmtData;
    SM_FileHandle fh;
    openPageFile((char*)bm->pageFile, &fh);
    Frame *current = dm->fhead;
    while(current!=NULL){
	if(current->dirty == 1){
		writeBlock(current->ph->pageNum, &fh, current->ph->data);
		current->dirty = 0; 
	}
		current = current->next;
    }
    closePageFile(&fh);
    return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    BM_DataManager *dm =(BM_DataManager *)bm->mgmtData;
    if(dm->copyPage[page->pageNum]!=NULL)
    dm->copyPage[page->pageNum]->dirty=1;
    return RC_OK;
	
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    BM_DataManager *dm = bm->mgmtData;
    if(dm->copyPage[page->pageNum]->fixCount!=0)
    dm->copyPage[page->pageNum]->fixCount--;
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    BM_DataManager *dm = bm->mgmtData;
    SM_FileHandle fh; 
    openPageFile(bm->pageFile, &fh);
    Frame *current = dm->copyPage[page->pageNum];
    writeBlock(current->ph->pageNum, &fh, current->ph->data);
    return RC_OK;
}

RC FIFO(BM_BufferPool *const bm, BM_PageHandle *const page,
                const PageNumber pageNum){
    BM_DataManager *dm = bm->mgmtData;
    SM_FileHandle fh;
    openPageFile(bm->pageFile, &fh);
    Frame *fr = dm->copyPage[pageNum];
	
    //If data is in the pool
    if(fr != NULL){
        page->pageNum = pageNum;
        page->data = fr->ph->data;
        fr->fixCount++;
    }
    //If data not in the pool, and no more space in the pool
    else if(dm->fempty == NULL){
        fr = dm->fhead;
	for(int i=0;;i++){
	if(fr!=NULL)
	fr=fr->next;
    }    
        fr->ph->pageNum = pageNum;
	dm->copyPage[pageNum] = fr;
        page->pageNum = pageNum;
        page->data = fr->ph->data;
        fr->fixCount = 1;
	readBlock(pageNum, &fh, fr->ph->data);
    //if still enough space in the pool
    }else{
        fr = dm->fempty;
	dm->fempty = dm->fempty->next;
        fr->ph->pageNum = pageNum;
        dm->copyPage[pageNum] = fr;
        page->pageNum = pageNum;
        page->data = fr->ph->data;
        fr->fixCount = 1;
	readBlock(pageNum, &fh, fr->ph->data);
    }
    closePageFile(&fh);
    return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum){
    return FIFO(bm,page,pageNum);
    return RC_OK;

}
// Statistics Interface
bool *getDirtyFlags (BM_BufferPool *const bm){	
    return FALSE;
}
int *getFixCounts(BM_BufferPool *const bm){
    return RC_OK;;
}
PageNumber *getFrameContents (BM_BufferPool *const bm){
    return RC_OK;;
}

int getNumReadIO (BM_BufferPool *const bm){
    return RC_OK;;
}

int getNumWriteIO (BM_BufferPool *const bm){
    return RC_OK;;
}

