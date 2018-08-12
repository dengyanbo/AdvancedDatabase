///////////////////////////////////////////////////////////////////////////////
//////////////////////////// CS 525 Summer 2018 ///////////////////////////////
////////////////////// Yanbo Deng, Xinghang Ma, Yitong Huang //////////////////
///////////////////////////////////////////////////////////////////////////////
**************************** Assignment 3 *************************************

********** HOW TO RUN **********
1. Go to the folder which contains source/ README.txt and Makefile
2. > make
3. > ./build/assign3.out
4. > make clean

********* Methods Description In buffer_mgr.c **********

/////////////////////////////
/*    Dealing with schemas  */
/////////////////////////////

1. getRecordSize
1) initial record size for data.
2) get record size from data.Different datatype got different cases
3) return recordSize.

2. createSchema
1) Allocate schema with spaces and different parameters. 
2) return schema .

3. freeSchema
1) free the schema been created
2) return RC_OK.


////////////////////////////////////////////////
/*    dealing with records and attribute values  */
////////////////////////////////////////////////

1. createRecord
1) create record then store them in data
2) Return RC_OK.

2. freeRecord
1) free the record in memory 
3) Return RC_OK. 

3. getAttr
1) check if attribute number is bigger than all attributes
2) get attributes value then calculate the distance to target position
3) add attribute size for different dataTypes then find attributes values.
4) copy from attributes to memory.Return RC_OK

4. setAttr
1) check if the value is right dataType
2) get attributes value then calculate the distance to target position
3) set data to the target position.
4) copy data from memory to attributes.Return RC_OK
