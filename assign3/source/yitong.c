#include "record_mgr.h"

// table and manager

// table and manager
RC initRecordManager(void *mgmtData)
{
    printf("Start to generate record manager!\n");
    return RC_OK;
}

RC shutdownRecordManager()
{
    printf("Shutdown record manager!\n");
    return RC_OK;
}

RC createTable(char *name, Schema *schema)
{
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