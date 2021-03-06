#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);
static void testWriteMethods(void);
static void testReadMethods(void);

/* main function running all tests */
int
main (void)
{
  testName = "";
  
  initStorageManager();

  testCreateOpenClose();
  testSinglePageContent();
  testWriteMethods();
  testReadMethods();

  return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Try to create, open, and close a page file */
void
testCreateOpenClose(void)
{
  SM_FileHandle fh;

  testName = "test create open and close methods";

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");

  TEST_DONE();
}

/* Try to create, open, and close a page file */
void
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  
  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");
    
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("reading first block\n");

  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  
  
  TEST_DONE();
}

/* my test for testing every methods in storage_mgr.c */
void
testWriteMethods(void){
  SM_FileHandle fh;
  SM_PageHandle ph;

  testName = "test all methods in storage_mgr.c";
  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  fprintf(stdout, "created and opened file\n");

  TEST_CHECK(appendEmptyBlock(&fh));
  fprintf(stdout, "append a new page to the file\n");

  char * temp = "current ";
  for(int i = 0,j = 0; i < PAGE_SIZE; ++i, ++j){
    if(j >= sizeof(temp)) j = 0;
    ph[i] = temp[j];
  }
  TEST_CHECK(writeCurrentBlock(&fh, ph));
  fprintf(stdout, "Written to current block!\n");

  TEST_CHECK(appendEmptyBlock(&fh));
  fprintf(stdout, "append a new page to the file\n");

  temp = "firstbl ";
  for(int i = 0,j = 0; i < PAGE_SIZE; ++i, ++j){
    if(j >= sizeof(temp)) j = 0;
    ph[i] = temp[j];
  }
  TEST_CHECK(writeBlock(0, &fh, ph));
  fprintf(stdout, "Written to the 1st block!\n");

  TEST_CHECK(ensureCapacity(10, &fh));
  fprintf(stdout, "Ensure the capacity is 10\n");

  TEST_CHECK(closePageFile(&fh));
  fprintf(stdout, "page file has been closed\n");

  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  

  free(ph);
  TEST_DONE();
}

void
testReadMethods(void){
  SM_FileHandle fh;
  SM_PageHandle ph;
  int numPages = 5;

  testName = "test all methods in storage_mgr.c";
  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  fprintf(stdout, "created and opened file\n");

  TEST_CHECK(ensureCapacity(numPages, &fh));
  fprintf(stdout, "Ensure the capacity is 10\n");

  for(int i = 0; i < numPages; ++i){
    memset(ph, i+'0', PAGE_SIZE);
    TEST_CHECK(writeBlock(i, &fh, ph));
    fprintf(stdout, "Write block No.%d with all %d\n", i, i);
  }
  
  SM_PageHandle tempPage = (SM_PageHandle) malloc(PAGE_SIZE);
  for(int i = 0; i < numPages; ++i){
    memset(tempPage, i + '0', PAGE_SIZE);
    TEST_CHECK(readBlock (i, &fh, ph));
    ASSERT_EQUALS_STRING(ph, tempPage, "This page is read correctly!");
  }
  fprintf(stdout, "Read blocks correctly\n");

  memset(tempPage, '0', PAGE_SIZE);
  TEST_CHECK(readFirstBlock (&fh, ph));
  ASSERT_EQUALS_STRING(ph, tempPage, "First page is read correctly!");

  memset(tempPage, (numPages - 1) + '0', PAGE_SIZE);
  TEST_CHECK(readLastBlock (&fh, ph));
  ASSERT_EQUALS_STRING(ph, tempPage, "Last page is read correctly!");

  fh.curPagePos = 2;
  memset(tempPage, '1', PAGE_SIZE);
  TEST_CHECK(readPreviousBlock (&fh, ph));
  ASSERT_EQUALS_STRING(ph, tempPage, "Previous page is read correctly!");

  memset(tempPage, '3', PAGE_SIZE);
  TEST_CHECK(readNextBlock (&fh, ph));
  ASSERT_EQUALS_STRING(ph, tempPage, "Next page is read correctly!");

  TEST_CHECK(closePageFile(&fh));
  fprintf(stdout, "page file has been closed\n");
  
  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  

  free(ph);
  free(tempPage);
  TEST_DONE();
}