#include "record_mgr.h"

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){
    if(rel == NULL) return RC_RM_NO_MORE_TUPLES;
    if(record == NULL) return RC_OK;
    

    return RC_OK;
}
RC deleteRecord (RM_TableData *rel, RID id);
RC updateRecord (RM_TableData *rel, Record *record);
RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
RC next (RM_ScanHandle *scan, Record *record);
RC closeScan (RM_ScanHandle *scan);