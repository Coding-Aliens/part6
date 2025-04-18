#include "catalog.h"
#include "query.h"
#include "heapfile.h"      // InsertFileScan, Record, RID
#include <cstring>         // memcpy, memset
#include <cstdlib>         // atoi, atof
#include <string>


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	// 1) Fetch all attributes of 'relation' to learn offsets + lengths
    int        totalAttrs;
    AttrDesc * allAttrs = nullptr;
    Status     rc = attrCat->getRelInfo(relation, totalAttrs, allAttrs);
    if (rc != OK) return rc;

    // 2) Reject NULLs: user must supply exactly every attribute
    if (attrCnt != totalAttrs) {
        delete[] allAttrs;
        return BADCATPARM;
    }

    // 3) Compute total record length = max(offset + len) over all attributes
    int reclen = 0;
    for (int i = 0; i < totalAttrs; i++) {
        int end = allAttrs[i].attrOffset + allAttrs[i].attrLen;
        if (end > reclen) reclen = end;
    }

    // 4) Build a temp buffer, zero‑initialized
    char *buf = new char[reclen];
    memset(buf, 0, reclen);

    // 5) For each supplied attrInfo, locate its catalog entry and copy the value
    for (int i = 0; i < attrCnt; i++) {
        // find matching AttrDesc in allAttrs[]
        const string want = attrList[i].attrName;
        int j = 0;
        for (; j < totalAttrs; j++) {
            if (want == allAttrs[j].attrName) break;
        }
        // safety check
        if (j == totalAttrs) {
            delete[] buf;
            delete[] allAttrs;
            return ATTRNOTFOUND;
        }

        // convert the C‑string attrValue to raw bytes
        if (allAttrs[j].attrType == INTEGER) {
			const char *s = static_cast<const char*>(attrList[i].attrValue);
			int v = atoi(s);
            memcpy(buf + allAttrs[j].attrOffset, &v, sizeof(int));
        }
        else if (allAttrs[j].attrType == FLOAT) {
			const char *s = static_cast<const char*>(attrList[i].attrValue);
            float v = atof(s);
            memcpy(buf + allAttrs[j].attrOffset, &v, sizeof(float));
        }
        else {
            // STRING: copy up to attrLen bytes (no null‑termination guaranteed)
            memcpy(buf + allAttrs[j].attrOffset,
                   attrList[i].attrValue,
                   allAttrs[j].attrLen);
        }
    }

    // 6) Open an inserter on the heapfile and insert
    InsertFileScan inserter(relation, rc);
    if (rc != OK) {
        delete[] buf;
        delete[] allAttrs;
        return rc;
    }
    RID     newRid;
    Record  newRec;
    newRec.data   = buf;
    newRec.length = reclen;
    inserter.insertRecord(newRec, newRid);

    // 7) cleanup
    delete[] buf;
    delete[] allAttrs;
    return OK;
}

