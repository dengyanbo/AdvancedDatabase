#include "record_mgr.h"

typedef struct RM_tablePtr
{
  int numPage;
  int numRecord;
  int numRecPerPage;
  int numInsert;

  BM_BufferPool *bp;
} RM_tablePtr;

static RM_TableData *table = NULL;

RC serializeFirstPage(Schema *schema, RM_tablePtr *tp)
{
  char *first = (char *)malloc(PAGE_SIZE);
  char *head = first;

  memcpy(first, &tp->numPage, sizeof(int));
  first += sizeof(int);
  memcpy(first, &tp->numRecord, sizeof(int));
  first += sizeof(int);
  memcpy(first, &tp->numRecPerPage, sizeof(int));
  first += sizeof(int);
  memcpy(first, &tp->numInsert, sizeof(int));
  first += sizeof(int);
  first = head;
  strcat(first, serializeSchema(schema));
  TEST_CHECK(createPageFile(table->name));
  SM_FileHandle* fh = (SM_FileHandle*) malloc(sizeof(SM_FileHandle));
  TEST_CHECK(openPageFile(table->name, fh));
  //write to the first page
  TEST_CHECK(writeBlock(0, fh, first));
  first = NULL;
  free(first);
  return RC_OK;
}

// table and manager
RC initRecordManager(void *mgmtData)
{
  table = (RM_TableData *)malloc(sizeof(RM_TableData));
  table->mgmtData = mgmtData;
  printf("Start to generate record manager!\n");
  return RC_OK;
}

RC shutdownRecordManager()
{
  free(table->name);
  free(table->schema);
  free(table->mgmtData);
  free(table);
  printf("Shutdown record manager!\n");
  return RC_OK;
}

RC createTable(char *name, Schema *schema)
{
  printf("start createTable!\n");
  createPageFile(name);
  SM_FileHandle fh;
  openPageFile(name, &fh);
  //use function serializeSchema in rm_serializer to retieve schema info
  char *getSchema = serializeSchema(schema);
  //get how many page to save schema info
  int schemaPageNum = (sizeof(getSchema) + PAGE_SIZE) / (PAGE_SIZE + 1);
  //allocate memory to save shema info, store shema page numberand schema info to this memory
  char *metadataPage = (char *)malloc(PAGE_SIZE);
  memset(metadataPage, 0, sizeof(PAGE_SIZE));
  memmove(metadataPage, &schemaPageNum, sizeof(schemaPageNum));
  memmove(metadataPage + sizeof(schemaPageNum), getSchema, strlen(getSchema));
  writeBlock(0, &fh, metadataPage);
  int i = 1;
  while (i < schemaPageNum)
  {
    memset(metadataPage, 0, sizeof(PAGE_SIZE));
    memmove(metadataPage, i * PAGE_SIZE + getSchema, PAGE_SIZE);
    writeBlock(i, &fh, metadataPage);
    free(metadataPage);
    i = i + 1;
  }
  RM_tablePtr* tp = (RM_tablePtr*) malloc(sizeof(RM_tablePtr));
  tp->numRecPerPage = 0;
  tp->numRecord = 0;
  tp->numPage = 0;
  tp->numInsert = 0;
  tp->bp = NULL;
  table->name = (char *)malloc(sizeof(name));
  table->name = name;
  table->schema = (Schema*)malloc(sizeof(schema));
  table->schema = schema;
  table->mgmtData = tp;
  TEST_CHECK(serializeFirstPage(schema, tp));
  printf("0 tp:nP = %d, nRPP = %d, nI = %d, nR = %d\n",tp->numPage, tp->numRecPerPage, tp->numInsert, tp->numRecord);
  printf("end createTable!\n");
  return RC_OK;
}

RC openTable(RM_TableData *rel, char *name)
{
  SM_FileHandle fh;
  openPageFile(name, &fh);
  BM_BufferPool *bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
  initBufferPool(bm, name, 1000, RS_FIFO, NULL);
  //get how many page to save schema info
  BM_PageHandle *ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
  pinPage(bm, ph, 0);
  int metadataPage = *ph->data;
  //allocate memory for schema and read it
  char *metadata = (char *)malloc(sizeof(char) * PAGE_SIZE);
  int i = 0;
  while (i < metadataPage)
  {
    readBlock(i, &fh, i * PAGE_SIZE + metadata);
    i = i + 1;
  }
  //initialize keySize and keys memory
  int keySize = 1;
  int *keys = (int *)malloc(sizeof(int));
  //check schema info
  char *attrInfo = strstr(metadata + 4, "<") + 1;
  printf("Schema has %s \n", attrInfo - 1);
  char *attrList = (char *)malloc(sizeof(char));
  memset(attrList, 0, sizeof(attrList));
  //read number of attributes and assign it to numAttr
  i = 0;
  while (*(attrInfo + i) != '>')
  {
    attrList[i] = attrInfo[i];
    i = i + 1;
  }
  int numAttr = atol(attrList);
  char **attrNames = (char **)malloc(sizeof(char *) * numAttr);
  free(attrList);
  //get posistion of "("
  attrInfo = strstr(attrInfo, "(") + 1;
  DataType *dataType = (DataType *)malloc(sizeof(DataType) * numAttr);
  int *typeLength = (int *)malloc(sizeof(int) * numAttr);
  i = 0;
  int j;
  //read datatype and typelenth from schema
  while (i < numAttr)
  {
    for (j = 0;; j++)
    {
      int size = 50;
      char *str = (char *)malloc(sizeof(char) * size);
      str = attrInfo + j;
      char *dest = (char *)malloc(sizeof(char) * size);
      strncpy(dest, str, 1);
      if (strcmp(dest, ":") == 0)
      {
        attrNames[i] = (char *)malloc(sizeof(char) * j);
        memcpy(attrNames[i], attrInfo, j);

        char *str1 = (char *)malloc(sizeof(char) * size);
        str1 = attrInfo + j + 2;
        char *dest1 = (char *)malloc(sizeof(char) * size);
        char *dest2 = (char *)malloc(sizeof(char) * size);
        char *dest3 = (char *)malloc(sizeof(char) * size);
        strncpy(dest1, str1, 6);
        strncpy(dest2, str1, 3);
        strncpy(dest3, str1, 5);

        if (strcmp(dest1, "STRING") == 0)
        {
          attrList = (char *)malloc(sizeof(char));
          int k = 0;
          while (*(attrInfo + j + 9 + k) != ']')
          {
            attrList[k] = attrInfo[k + j + 9];
            k = k + 1;
          }
          dataType[i] = DT_STRING;
          typeLength[i] = atol(attrList);
        }
        else if (strcmp(dest2, "INT") == 0)
        {
          dataType[i] = DT_INT;
          typeLength[i] = 0;
        }
        else if (strcmp(dest3, "FLOAT") == 0)
        {
          dataType[i] = DT_FLOAT;
          typeLength[i] = 0;
        }
        else
        {
          dataType[i] = DT_BOOL;
          typeLength[i] = 0;
        }
        //check if read to the last attribute
        if (i != numAttr - 1)
        {
          attrInfo = strstr(attrInfo, ",");
          attrInfo = attrInfo + 2;
        }
        break;
      }
    }
    i++;
  }
  //assign schema details
  Schema *schema = createSchema(numAttr, attrNames, dataType, typeLength, keySize, keys);
  rel->name = name;
  rel->schema = schema;
  rel->mgmtData = bm;
  RM_tablePtr* tp = (RM_tablePtr*) table->mgmtData;
  tp->bp = bm;
  printf("end openTable!\n");
  return RC_OK;
}

RC closeTable(RM_TableData *rel)
{

  shutdownBufferPool((BM_BufferPool *)rel->mgmtData);

  freeSchema(rel->schema);
  free(rel->mgmtData);
  int i = 0;
  while (i < rel->schema->numAttr)
  {
    free(rel->schema->attrNames[i]);
    i = i + 1;
  }
  return RC_OK;
}

RC deleteTable(char *name)
{
  remove(name);
  return RC_OK;
}

int getNumTuples(RM_TableData *rel)
{
  BM_PageHandle *ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
  int tupleNum;
  pinPage(rel->mgmtData, ph, 0);
  return *ph->data + sizeof(int);
}

// handling records in a table
RC insertRecord(RM_TableData *rel, Record *record)
{
  if (rel == NULL)
    return RC_BAD_PARAMETER;
  if (record == NULL)
    return RC_BAD_PARAMETER;

  RM_tablePtr *tp = (RM_tablePtr *)table->mgmtData;
printf("tp:nP = %d, nRPP = %d, nI = %d, nR = %d\n",             \
    tp->numPage, tp->numRecPerPage, tp->numInsert, tp->numRecord);
  int recordSize = getRecordSize(rel->schema) + 3;
  int totalSlots = PAGE_SIZE / recordSize;
  printf("total = %d, recordSize = %d\n", totalSlots, recordSize);
  tp->numRecPerPage = totalSlots;
  tp->numRecord += 1;
  tp->numInsert += 1;
  int slot = tp->numInsert % tp->numRecPerPage;
  if (slot == 1)
    tp->numPage += 1;
  record->id.page = tp->numPage;
  if (slot == 0)
    record->id.slot = tp->numRecPerPage - 1;
  else
    record->id.slot = slot - 1;
  BM_BufferPool *bm = tp->bp;
  BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
  page->data = (char *)malloc(PAGE_SIZE);
  TEST_CHECK(pinPage(bm, page, record->id.page));
  char *ptr = page->data + recordSize * record->id.slot;
  memcpy(ptr, record->data, recordSize);
  TEST_CHECK(markDirty(bm, page));
  TEST_CHECK(unpinPage(bm, page));
  TEST_CHECK(forcePage(bm, page));
  table->mgmtData = tp;
  rel = table;

  return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id)
{
  int dPage = id.page;
  int dSlot = id.slot;
  RM_tablePtr *tp = (RM_tablePtr *)table->mgmtData;

  BM_BufferPool *bm = tp->bp;

  BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
  page->data = (char *)malloc(PAGE_SIZE);
  TEST_CHECK(pinPage(bm, page, dPage));
  char *ptr = page->data + tp->numRecPerPage * dSlot;
  memset(ptr, '\0', tp->numRecPerPage);
  TEST_CHECK(markDirty(bm, page));
  TEST_CHECK(unpinPage(bm, page));
  TEST_CHECK(forcePage(bm, page));

  tp->numRecord -= 1;
  table->mgmtData = tp;
  rel = table;

  return RC_OK;
}
RC updateRecord(RM_TableData *rel, Record *record)
{
  if (rel == NULL)
    return RC_BAD_PARAMETER;
  if (record == NULL)
    return RC_BAD_PARAMETER;

  RM_tablePtr *tp = (RM_tablePtr *)table->mgmtData;
  BM_BufferPool *bm = tp->bp;
  int recordSize = getRecordSize(rel->schema);
  int totalSlots = tp->numRecPerPage;

  BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
  page->data = (char *)malloc(PAGE_SIZE);
  TEST_CHECK(pinPage(bm, page, record->id.page));

  char *ptr = page->data + recordSize * record->id.slot;
  memcpy(ptr, record->data, recordSize);

  TEST_CHECK(markDirty(bm, page));
  TEST_CHECK(unpinPage(bm, page));
  TEST_CHECK(forcePage(bm, page));

  return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record)
{
  if (rel == NULL)
    return RC_BAD_PARAMETER;
  if (record == NULL)
    return RC_BAD_PARAMETER;

  RM_tablePtr *tp = (RM_tablePtr *)table->mgmtData;
  BM_BufferPool *bm = tp->bp;
  int recordSize = getRecordSize(rel->schema);
  int totalSlots = tp->numRecPerPage;
  record->id = id;

  BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
  page->data = (char *)malloc(PAGE_SIZE);
  TEST_CHECK(pinPage(bm, page, record->id.page));

  char *ptr = page->data + recordSize * record->id.slot;
  memcpy(record->data, ptr, recordSize);

  return RC_OK;
}

// scans
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
  if (rel == NULL)
    return RC_BAD_PARAMETER;
  if (scan == NULL)
    return RC_BAD_PARAMETER;
  if (cond == NULL)
    return RC_BAD_PARAMETER;

  scan->rel = rel;
  RM_ScanData *scanData = (RM_ScanData *)malloc(sizeof(RM_ScanData));
  scanData->numScan = 0;
  scanData->cond = cond;
  scanData->id.page = 1;
  scanData->id.slot = 0;

  scan->mgmtData = scanData;

  return RC_OK;
}
RC next(RM_ScanHandle *scan, Record *record)
{
  if (scan == NULL)
    return RC_BAD_PARAMETER;
  if (record == NULL)
    return RC_BAD_PARAMETER;

  RM_ScanData *scanData = (RM_ScanData *)scan->mgmtData;
  RM_TableData *rel = scan->rel;
  RM_tablePtr *tp = (RM_tablePtr *)table->mgmtData;
  Value *val = (Value *)malloc(sizeof(Value));
  val->v.boolV = FALSE;

  while (!val->v.boolV)
  {
    if (scanData->numScan == tp->numRecord)
      return RC_RM_NO_MORE_TUPLES;
    TEST_CHECK(getRecord(table, scanData->id, record));
    evalExpr(record, table->schema, scanData->cond, &val);
    scanData->numScan += 1;
    scanData->id.slot += 1;
    if (scanData->id.slot == tp->numRecPerPage)
    {
      scanData->id.page += 1;
      scanData->id.slot = 0;
    }
  }

  scan->mgmtData = scanData;

  return RC_OK;
}
RC closeScan(RM_ScanHandle *scan)
{
  if (scan == NULL)
    return RC_BAD_PARAMETER;
  scan->rel = NULL;
  free(scan->mgmtData);
  free(scan);

  return RC_OK;
}

#include "record_mgr.h"

// dealing with schemas
int getRecordSize(Schema *schema)
{
  //initial record size
  int recordSize = 0;
  DataType *dataType;
  dataType = schema->dataTypes;
  //get the record size from the data
  for (int i = 0; i < schema->numAttr; i++)
  {

    switch (dataType[i])
    {
      //for Strings dataType
    case 0:
    {
      recordSize += schema->typeLength[i] * sizeof(char);
      break;
    }
      //for Int dataType

    case 1:
    {
      recordSize += sizeof(int);
      break;
    }
      //for Float dataType

    case 2:
    {
      recordSize += sizeof(float);
      break;
    }
      //for Boolean dataType
    case 3:
    {
      recordSize += sizeof(bool);
      break;
    }
    default:
      break;
    }
  }

  return recordSize;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
  // Allocate schema with spaces and parameters
  Schema *schema = (Schema *)malloc(sizeof(Schema));
  schema->numAttr = numAttr;
  schema->attrNames = attrNames;
  schema->dataTypes = dataTypes;
  schema->typeLength = typeLength;
  schema->keySize = keySize;
  schema->keyAttrs = keys;

  return schema;
}

RC freeSchema(Schema *schema)
{
  // Free the schema
  free(schema->dataTypes);
  free(schema->keyAttrs);
  free(schema->typeLength);
  free(schema->attrNames);
  free(schema);

  return RC_OK;
}

RC createRecord(Record **record, Schema *schema)
{
  //create record and store them in data
  Record *rec1 = (Record *)malloc(sizeof(Record));
  *record = rec1;
  rec1->data = (char *)malloc(getRecordSize(schema));
  return RC_OK;
}

RC freeRecord(Record *record)
{
  //free the record in memory
  free(record->data);
  free(record);
  return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
  //if attribute number bigger than all attribute
  if (attrNum >= schema->numAttr)
  {
    return RC_BAD_PARAMETER;
  }
  // get attributes value then calculate the distance to target position
  Value *Attribute = (Value *)malloc(sizeof(Value));
  int position = 0;
  int i;
  DataType *datatype = schema->dataTypes;
  //add attributes size
  for (i = 0; i < attrNum; i++)
  {
    switch (datatype[i])
    {
      //string dataType
    case 0:
    {
      position += (schema->typeLength[i]) * sizeof(char);
      break;
    }
      //int dataType
    case 1:
    {
      position += sizeof(int);
      break;
    }
      //float dataType
    case 2:
    {
      position += sizeof(float);
      break;
    }
      //boolean dataType
    case 3:
    {
      position += sizeof(bool);
      break;
    }
    default:
      break;
    }
    // find attributes value
    Attribute->dt = datatype[attrNum];
    *value = Attribute;
    switch (datatype[attrNum])
    {
      //String dataType
    case 0:
    {
      //giving the space to attributes
      Attribute->v.stringV = (char *)calloc(schema->typeLength[attrNum], sizeof(char));
      //memory copy from attributes
      memcpy(Attribute->v.stringV, ((char *)record->data) + position, schema->typeLength[attrNum] * sizeof(char));

      break;
    }
      //int dataType
    case 1:
    {
      memcpy(&(Attribute->v.intV), record->data + position, sizeof(int));
      break;
    }
      //float dataType
    case 2:
    {
      memcpy(&(Attribute->v.floatV), record->data + position, sizeof(float));
      break;
    }
      //boolean datatype
    case 3:
      memcpy(&(Attribute->v.boolV), record->data + position, sizeof(bool));
      break;
    }
    return RC_OK;
  }
}
//calcutlate the position to target attribute size and dataType
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
  //check the value if not equal to any dataType
  if (value->dt != schema->dataTypes[attrNum])
  {
    return RC_RM_UNKOWN_DATATYPE;
  }

  int position = 0;
  for (int i = 0; i < attrNum; i++)
  {

    switch (schema->dataTypes[i])
    {
      // String dataType
    case 0:
    {
      position += schema->typeLength[i];
      break;
    }
    // Int dataType
    case 1:
    {
      position += sizeof(int);
      break;
    }
    // float dataType
    case 2:
    {
      position += sizeof(float);
      break;
    }
    //boolean dataType
    case 3:
    {
      position += sizeof(bool);
      break;
    }
    }
  }
  char *data = record->data + position;

  switch (schema->dataTypes[attrNum])
  {
  case 0:
  {
    // memory copy to the record attributes
    memcpy(record->data + position, value->v.stringV, schema->typeLength[attrNum]);
    data += schema->typeLength[attrNum];
    break;
  }

  case 1:
  {
    *(int *)data = value->v.intV;
    data += sizeof(int);
    break;
  }

  case 2:
  {
    *(float *)data = value->v.floatV;
    data += sizeof(float);
    break;
  }

  case 3:
  {
    *(bool *)data = value->v.boolV;
    data += sizeof(bool);
    break;
  }

  default:
    printf("undefined dataType \n");
    break;
  }

  return RC_OK;
}
