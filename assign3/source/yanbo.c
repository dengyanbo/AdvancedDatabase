#include "record_mgr.h"

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){
    if(rel == NULL) return RC_RM_NO_MORE_TUPLES;
    if(record == NULL) return RC_OK;
    

    return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id){return RC_OK;}
RC updateRecord (RM_TableData *rel, Record *record){return RC_OK;}
RC getRecord (RM_TableData *rel, RID id, Record *record){return RC_OK;}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){return RC_OK;}
RC next (RM_ScanHandle *scan, Record *record){return RC_OK;}
RC closeScan (RM_ScanHandle *scan){return RC_OK;}