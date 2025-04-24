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
	Status rc;

    // 1) Open the scan on the relation
    HeapFileScan scanner(relation, rc);
    if (rc != OK) 
        return rc;            // e.g. RELNOTFOUND, FILEOPEN error

    // 2) Decide whether it’s filtered or unfiltered
    if (attrName.empty()) {
        // --- DELETE ALL: unfiltered scan
        // startScan(offset=0, length=0, type=STRING, filter=NULL, op=EQ)
        // (engine treats length=0||filter==NULL as “no filter”)
        rc = scanner.startScan(0, 0, STRING, nullptr, EQ);
    }
    else {
        // --- DELETE … WHERE:
        AttrDesc desc;
        rc = attrCat->getInfo(relation, attrName, desc);
        if (rc != OK)
            return rc;        // ATTRNOTFOUND or RELNOTFOUND

        rc = scanner.startScan(
            desc.attrOffset,
            desc.attrLen,
            type,
            attrValue,
            op
        );
    }
    if (rc != OK) 
        return rc;            // BADSCANPARM, etc.

    // 3) Walk the scan, deleting every matching record
    RID rid;
    while (scanner.scanNext(rid) == OK) {
        scanner.deleteRecord();
    }
    scanner.endScan();
    return OK;



}


