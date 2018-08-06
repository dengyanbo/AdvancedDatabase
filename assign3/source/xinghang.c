
#include "record_mgr.h"


// dealing with schemas
extern int getRecordSize (Schema *schema)
{
  //initial record size
  int recordSize = 0;
  DataType *dataType;
  dataType = schema->dataTypes;
  //get the record size from the data
  for(int i = 0; i < schema->numAttr; i++) {

      switch(dataType[i]) {
    //for Strings dataType
      case 1:
      {
          recordSize += schema->typeLength[i] * sizeof(char);
          break;
        }
    //for Int dataType

      case 2:
      {
          recordSize += sizeof(int);
          break;
        }
    //for Float dataType

      case 3:
      {
          recordSize += sizeof(float);
          break;
        }
    //for Boolean dataType
      case 4:
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

extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
      // Allocate schema with spaces and parameters
      Schema *schema = (Schema *) malloc(sizeof(Schema));
      schema->numAttr = numAttr;
      schema->attrNames = attrNames;
      schema->dataTypes = dataTypes;
      schema->typeLength = typeLength;
      schema->keySize = keySize;
      schema->keyAttrs = keys;

      return schema;
}

extern RC freeSchema (Schema *schema)
{
      // Free the schema
      free(schema->dataTypes);
      free(schema->keyAttrs);
      free(schema->typeLength);
      free(schema->attrNames);
      free(schema);

      return RC_OK;
}


extern RC createRecord (Record **record, Schema *schema)
{
      //create record and store them in data
      Record *rec1=(Record *)malloc(sizeof(Record));
      *record=rec1;
      rec1->data=(char *) malloc (getRecordSize(schema));
      return RC_OK;
}

extern RC freeRecord (Record *record)
{
      //free the record in memory
      free(record->data);
      free(record);
      return RC_OK;
}

extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
  //if attribute number bigger than all attribute
  if (attrNum >= schema->numAttr)
  {
    return RC_BAD_PARAMETER;
  }
  // get attributes value then calculate the distance to target position
  Value *Attribute=(Value *)malloc(sizeof(Value));
  int position=0; int i;
  DataType *datatype=schema->dataTypes;
  //add attributes size
  for (i = 0; i < attrNum; i++)
  {
      switch (datatype[i])
      {
        //string dataType
          case 1:
          {
              position += (schema->typeLength[i]) * sizeof(char);
              break;
          }
        //int dataType
          case 2:
          {
              position += sizeof(int);
              break;
          }
        //float dataType
          case 3:
          {
              position += sizeof(float);
              break;
          }
        //boolean dataType
          case 4:
          {
              position += sizeof(bool);
              break;
          }
          default:
            break;

      }
      // find attributes value
      Attribute->dt=datatype[attrNum];
      *value=Attribute;
      switch(datatype[attrNum]){
        //String dataType
    	case 1:
      {
        //giving the space to attributes
        Attribute->v.stringV=(char *)calloc(schema->typeLength[attrNum],sizeof(char));
        //memory copy from attributes
        memcpy(Attribute->v.stringV,((char *)record->data)+position,schema->typeLength[attrNum]*sizeof(char));

    			break;
        }
        //int dataType
      case 2:
      {
      	memcpy(&(Attribute->v.intV), record->data + position, sizeof(int));
          break;
        }
        //float dataType
    	case 3:
      {
    			memcpy(&(Attribute->v.floatV), record->data + position, sizeof(float));
    			break;
        }
        //boolean datatype
    	case 4:
    			memcpy(&(Attribute->v.boolV), record->data + position, sizeof(bool));
    			break;
    	}
    	return RC_OK;
}
}
//calcutlate the position to target attribute size and dataType
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
//check the value if not equal to any dataType
  if (value->dt != schema->dataTypes[attrNum])
  {
         return RC_RM_UNKOWN_DATATYPE;
 }

  int position = 0;
  for(int i = 0; i < attrNum; i++)
  {

      switch (schema->dataTypes[i]){
          // String dataType
         case 1:
          {
            position +=  schema->typeLength[i];
            break;
          }
          // Int dataType
          case 2:
          {
            position +=  sizeof(int);
            break;
          }
          // float dataType
          case 3:
          {
            position +=  sizeof(float);
            break;
          }
          //boolean dataType
          case 4:
          {
            position +=  sizeof(bool);
            break;
          }
  }
}
  char *data= record->data + position;

        switch(schema->dataTypes[attrNum])
        {
          case 1:
          {
            // memory copy to the record attributes
            memcpy(record->data + position, value->v.stringV,schema->typeLength[attrNum]);
            data += schema->typeLength[attrNum];
            break;
          }

          case 2:
          {
            *(int *) data = value->v.intV;
            data +=  sizeof(int);
            break;
          }

          case 3:
          {
            *(float *) data = value->v.floatV;
            data +=  sizeof(float);
            break;
          }

          case 4:
          {
            *(bool *) data = value->v.boolV;
            data +=  sizeof(bool);
            break;
          }

          default:
            printf("undefined dataType \n");
            break;
        }

return RC_OK;

}
