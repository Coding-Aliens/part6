#include "catalog.h"     // RelCatalog, AttrCatalog, AttrDesc, attrInfo
#include "query.h"       // Status, Operator, attrInfo
#include "heapfile.h"    // HeapFile
#include <iostream>
#include <cstring>       // memcpy
#include <cstdlib>       // atoi, atof
#include "stdio.h"
#include "stdlib.h"

// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	// 1) build projection descriptors
    AttrDesc *projAttrs = new AttrDesc[projCnt];
    for (int i = 0; i < projCnt; i++) {
        Status rc = attrCat->getInfo(
            string(projNames[i].relName),
            string(projNames[i].attrName),
            projAttrs[i]
        );
        if (rc != OK) {
            delete[] projAttrs;
            return rc;
        }
    }

    // 2) build optional selection descriptor
    AttrDesc  selAttrDesc;
    AttrDesc *pSel = nullptr;
    if (attr) {
        Status rc = attrCat->getInfo(
            string(attr->relName),
            string(attr->attrName),
            selAttrDesc
        );
        if (rc != OK) {
            delete[] projAttrs;
            return rc;
        }
        pSel = &selAttrDesc;
    }

    // 3) compute input‐record length
    int        baseAttrCnt;
    AttrDesc  *allAttrs = nullptr;
    Status     rc = attrCat->getRelInfo(
        string(projNames[0].relName),
        baseAttrCnt,
        allAttrs
    );
    if (rc != OK) {
        delete[] projAttrs;
        return rc;
    }
    int reclen = 0;
    for (int i = 0; i < baseAttrCnt; i++) {
        int end = allAttrs[i].attrOffset + allAttrs[i].attrLen;
        if (end > reclen) reclen = end;
    }
    delete[] allAttrs;

    // 4) perform the scan + projection
    rc = ScanSelect(result, projCnt, projAttrs, pSel, op, attrValue, reclen);
    delete[] projAttrs;
    return rc;

}


const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	Status st;

    // 1) open the input scan
    HeapFileScan scanner(
        projNames[0].relName,  // scan on the base relation
        st
    );
    if (st != OK) return st;

    // 2) start filtered (or full) scan
    st = scanner.startScan(
        attrDesc      ? attrDesc->attrOffset : 0,
        attrDesc      ? attrDesc->attrLen    : 0,
        attrDesc      ? (Datatype)attrDesc->attrType : STRING,
        filter,
        op
    );
    if (st != OK) return st;

    // 3) open the output inserter
    InsertFileScan inserter(result, st);
    if (st != OK) return st;

    // 4) allocate buffers
    char * inBuf  = new char[reclen];
    int    outLen = 0;
    for (int i = 0; i < projCnt; i++)
        outLen += projNames[i].attrLen;
    char * outBuf = new char[outLen];

    // 5) loop: scan -> project -> insert
    RID     inRid, outRid;
    Record  inRec, outRec;
    while (scanner.scanNext(inRid) == OK) {
        scanner.getRecord(inRec);
        // copy raw bytes into contiguous inBuf
        memcpy(inBuf, inRec.data, inRec.length);

        // project fields
        int dst = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(
                outBuf + dst,
                inBuf  + projNames[i].attrOffset,
                projNames[i].attrLen
            );
            dst += projNames[i].attrLen;
        }

        // prepare outRec and insert
        outRec.data   = outBuf;
        outRec.length = outLen;
        inserter.insertRecord(outRec, outRid);
    }

    delete[] inBuf;
    delete[] outBuf;
    return OK;

}
