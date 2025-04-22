#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	// 1) Look up the attribute to filter on
    AttrDesc filterDesc;
    Status rc = attrCat->getInfo(relation, attrName, filterDesc);
    if (rc != OK) {
        return rc;              // RELNOTFOUND or ATTRNOTFOUND
    }

    // 2) Open a heap‐file scan on the relation
    HeapFileScan scanner(relation, rc);
    if (rc != OK) {
        return rc;              // e.g. BADFILE, FILENOTOPEN
    }

    // 3) Start a filtered scan on the given attribute
    rc = scanner.startScan(
        filterDesc.attrOffset,
        filterDesc.attrLen,
        type,
        attrValue,
        op
    );
    if (rc != OK) {
        return rc;              // e.g. BADSCANPARM
    }

    // 4) Walk through matching RIDs and delete each tuple
    RID rid;
    while (scanner.scanNext(rid) == OK) {
        scanner.deleteRecord();
    }

    // 5) Done
    return OK;



}


