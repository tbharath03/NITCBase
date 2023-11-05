#include "BPlusTree.h"

#include <cstring>
#include <stdio.h>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op)
{
    // declare searchIndex which will be used to store search index for attrName.
    IndexId searchIndex;

    /* get the search index corresponding to attribute with name attrName
       using AttrCacheTable::getSearchIndex(). */
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);

    AttrCatEntry attrCatEntry;
    /* load the attribute cache entry into attrCatEntry using
     AttrCacheTable::getAttrCatEntry(). */
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // declare variables block and index which will be used during search
    int block, index;

    if (/* searchIndex == {-1, -1}*/ searchIndex.block == -1 && searchIndex.index == -1)
    {
        // (search is done for the first time)

        // start the search from the first entry of root.
        block = attrCatEntry.rootBlock;
        index = 0;

        if (/* attrName doesn't have a B+ tree (block == -1)*/ block == -1)
        {
            return RecId{-1, -1};
        }
    }
    else
    {
        /*a valid searchIndex points to an entry in the leaf index of the attribute's
        B+ Tree which had previously satisfied the op for the given attrVal.*/

        block = searchIndex.block;
        index = searchIndex.index + 1; // search is resumed from the next index.

        // load block into leaf using IndLeaf::IndLeaf().
        IndLeaf leaf(block);

        // declare leafHead which will be used to hold the header of leaf.
        HeadInfo leafHead;

        // load header into leafHead using BlockBuffer::getHeader().
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries)
        {
            /* (all the entries in the block has been searched; search from the
            beginning of the next leaf index block. */

            // update block to rblock of current block and index to 0.
            block = leafHead.rblock;
            index = 0;

            if (block == -1)
            {
                // (end of linked list reached - the search is done.)
                return RecId{-1, -1};
            }
        }
    }

    /******  Traverse through all the internal nodes according to value
             of attrVal and the operator op                             ******/

    /* (This section is only needed when
        - search restarts from the root block (when searchIndex is reset by caller)
        - root is not a leaf
        If there was a valid search index, then we are already at a leaf block
        and the test condition in the following loop will fail)
    */

    while (/* block is of type IND_INTERNAL */ StaticBuffer::getStaticBlockType(block) == IND_INTERNAL)
    { // use StaticBuffer::getStaticBlockType()

        // load the block into internalBlk using IndInternal::IndInternal().
        IndInternal internalBlk(block);

        HeadInfo intHead;

        // load the header of internalBlk into intHead using BlockBuffer::getHeader()
        internalBlk.getHeader(&intHead);
        // declare intEntry which will be used to store an entry of internalBlk.
        InternalEntry intEntry;

        if (/* op is one of NE, LT, LE */ op == NE || op == LT || op == LE)
        {
            /*
            - NE: need to search the entire linked list of leaf indices of the B+ Tree,
            starting from the leftmost leaf index. Thus, always move to the left.

            - LT and LE: the attribute values are arranged in ascending order in the
            leaf indices of the B+ Tree. Values that satisfy these conditions, if
            any exist, will always be found in the left-most leaf index. Thus,
            always move to the left.
            */

            // load entry in the first slot of the block into intEntry
            // using IndInternal::getEntry().
            internalBlk.getEntry(&intEntry, 0);

            block = intEntry.lChild;
        }
        else
        {
            /*
            - EQ, GT and GE: move to the left child of the first entry that is
            greater than (or equal to) attrVal
            (we are trying to find the first entry that satisfies the condition.
            since the values are in ascending order we move to the left child which
            might contain more entries that satisfy the condition)
            */

            /*
             traverse through all entries of internalBlk and find an entry that
             satisfies the condition.
             if op == EQ or GE, then intEntry.attrVal >= attrVal
             if op == GT, then intEntry.attrVal > attrVal
             Hint: the helper function compareAttrs() can be used for comparing
            */
            int ind = 0;
            int flag = 0;
            while (ind < intHead.numEntries)
            {
                internalBlk.getEntry(&intEntry, ind);
                int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType);
                if (/* such an entry is found*/ ((op==EQ || op==GE) && cmpVal>=0) || (op==GT && cmpVal>0))
                {
                    // move to the left child of that entry

                    flag = 1;
                    break; // left child of the entry
                }
                ind++;
            }
            if (flag == 1)
            {
                block = intEntry.lChild;
            }
            else
            {
                block = intEntry.rChild;
            }
            //index = 0;
        }
    }

    // NOTE: `block` now has the block number of a leaf index block.

    /******  Identify the first leaf index entry from the current position
                that satisfies our condition (moving right)             ******/
    //! index=0;
    while (block != -1)
    {
        // load the block into leafBlk using IndLeaf::IndLeaf().
        IndLeaf leafBlk(block);
        HeadInfo leafHead;

        // load the header to leafHead using BlockBuffer::getHeader().
        leafBlk.getHeader(&leafHead);
        // declare leafEntry which will be used to store an entry from leafBlk
        Index leafEntry;

        while (/*index < numEntries in leafBlk*/ index < leafHead.numEntries)
        {

            // load entry corresponding to block and index into leafEntry
            // using IndLeaf::getEntry().
            leafBlk.getEntry(&leafEntry, index);

            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType); /* comparison between leafEntry's attribute value /////////////ATTRTYPE
                               and input attrVal using compareAttrs()*/
            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0))
            {
                // (entry satisfying the condition found)

                // set search index to {block, index}
                IndexId sIndex;
                sIndex.block = block;
                sIndex.index = index;
                AttrCacheTable::setSearchIndex(relId, attrName, &sIndex);

                // return the recId {leafEntry.block, leafEntry.slot}.
                return RecId{leafEntry.block, leafEntry.slot};
            }
            else if ((op == EQ || op == LE || op == LT) && cmpVal > 0)
            {
                /*future entries will not satisfy EQ, LE, LT since the values
                    are arranged in ascending order in the leaves */

                // return RecId {-1, -1};
                return RecId{-1, -1};
            }

            // search next index.
            ++index;
        }

        /*only for NE operation do we have to check the entire linked list;
        for all the other op it is guaranteed that the block being searched
        will have an entry, if it exists, satisying that op. */
        if (op != NE) /// To find each entry which satisfies NE.     Even if
        {
            break;
        }

        // block = next block in the linked list, i.e., the rblock in leafHead.
        // update index to 0.
        block = leafHead.rblock;
        index = 0;
    }

    // no entry satisying the op was found; return the recId {-1,-1}
    return RecId{-1, -1};
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE])
{

    // if relId is either RELCAT_RELID or ATTRCAT_RELID:
    //     return E_NOTPERMITTED;
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
    {
        return E_NOTPERMITTED;
    }

    // get the attribute catalog entry of attribute `attrName`
    // using AttrCacheTable::getAttrCatEntry()
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // if getAttrCatEntry fails
    //     return the error code from getAttrCatEntry
    if (ret != SUCCESS)
    {
        return ret;
    }

    if (/* an index already exists for the attribute (check rootBlock field) */ attrCatEntry.rootBlock != -1)
    {
        return SUCCESS;
    }

    /******Creating a new B+ Tree ******/

    // get a free leaf block using constructor 1 to allocate a new block
    IndLeaf rootBlockBuf;

    //* debug begin

    // HeadInfo rootHeader;
    // rootBlockBuf.getHeader(&rootHeader);
    // printf("debug\n");

    //* debug end

    // (if the block could not be allocated, the appropriate error code
    //  will be stored in the blockNum member field of the object)

    // declare rootBlock to store the blockNumber of the new leaf block
    int rootBlock = rootBlockBuf.getBlockNum();

    // if there is no more disk space for creating an index
    if (rootBlock == E_DISKFULL)
    {
        return E_DISKFULL;
    }
    attrCatEntry.rootBlock = rootBlock;
    ret = AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS)
    {
        return ret;
    }

    RelCatEntry relCatEntry;

    // load the relation catalog entry into relCatEntry
    // using RelCacheTable::getRelCatEntry().
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int block = /* first record block of the relation */ relCatEntry.firstBlk;

    /***** Traverse all the blocks in the relation and insert them one
           by one into the B+ Tree *****/
    while (block != -1)
    {

        // declare a RecBuffer object for `block` (using appropriate constructor)
        RecBuffer BLOCK(block);

        unsigned char slotMap[relCatEntry.numSlotsPerBlk];

        // load the slot map into slotMap using RecBuffer::getSlotMap().
        BLOCK.getSlotMap(slotMap);

        // for every occupied slot of the block
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////s
        for (int slot = 0; slot < relCatEntry.numSlotsPerBlk; slot++)
        {
            if (slotMap[slot] == SLOT_UNOCCUPIED)
            {
                continue;
            }
            Attribute record[relCatEntry.numAttrs];
            // load the record corresponding to the slot into `record`
            // using RecBuffer::getRecord().
            BLOCK.getRecord(record, slot);

            // declare recId and store the rec-id of this record in it
            // RecId recId{block, slot};
            RecId recId{block, slot};

            // insert the attribute value corresponding to attrName from the record
            // into the B+ tree using bPlusInsert.
            // (note that bPlusInsert will destroy any existing bplus tree if
            // insert fails i.e when disk is full)
            // retVal = bPlusInsert(relId, attrName, attribute value, recId);
            int retVal = bPlusInsert(relId, attrName, record[attrCatEntry.offset], recId);

            // if (retVal == E_DISKFULL) {
            //     // (unable to get enough blocks to build the B+ Tree.)
            //     return E_DISKFULL;
            // }
            if (retVal == E_DISKFULL)
            {
                return E_DISKFULL;
            }
        }

        // get the header of the block using BlockBuffer::getHeader()
        HeadInfo head;
        BLOCK.getHeader(&head);

        // set block = rblock of current block (from the header)
        block = head.rblock;
    }

    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum)
{
    if (/*rootBlockNum lies outside the valid range [0,DISK_BLOCKS-1]*/ rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS)
    {
        return E_OUTOFBOUND;
    }

    int type = StaticBuffer::getStaticBlockType(rootBlockNum); /* type of block (using StaticBuffer::getStaticBlockType())*/

    if (type == IND_LEAF)
    {
        // declare an instance of IndLeaf for rootBlockNum using appropriate
        // constructor
        IndLeaf leafBlock(rootBlockNum);
        // release the block using BlockBuffer::releaseBlock().
        leafBlock.releaseBlock();
        return SUCCESS;
    }
    else if (type == IND_INTERNAL)
    {
        // declare an instance of IndInternal for rootBlockNum using appropriate
        // constructor
        IndInternal internalBlock(rootBlockNum);

        // load the header of the block using BlockBuffer::getHeader().
        HeadInfo head;
        internalBlock.getHeader(&head);

        /*iterate through all the entries of the internalBlk and destroy the lChild
        of the first entry and rChild of all entries using BPlusTree::bPlusDestroy().
        (the rchild of an entry is the same as the lchild of the next entry.
        take care not to delete overlapping children more than once )*/
        int index = 0;
        while (index < head.numEntries)
        {
            InternalEntry intEntry;
            internalBlock.getEntry(&intEntry, index);
            if (index == 0)
            {
                BPlusTree::bPlusDestroy(intEntry.lChild);
            }
            BPlusTree::bPlusDestroy(intEntry.rChild);
            index++;
        }

        // release the block using BlockBuffer::releaseBlock().
        internalBlock.releaseBlock();
        return SUCCESS;
    }
    else
    {
        // (block is not an index block.)
        return E_INVALIDBLOCK;
    }
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId)
{
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // if getAttrCatEntry() failed
    //     return the error code
    if (ret != SUCCESS)
    {
        return ret;
    }

    int blockNum = /* rootBlock of B+ Tree (from attrCatEntry) */ attrCatEntry.rootBlock;

    if (/* there is no index on attribute (rootBlock is -1) */ blockNum == -1)
    {
        return E_NOINDEX;
    }

    // find the leaf block to which insertion is to be done using the
    // findLeafToInsert() function

    int leafBlkNum = findLeafToInsert(blockNum, attrVal, attrCatEntry.attrType); /* findLeafToInsert(root block num, attrVal, attribute type) */

    // insert the attrVal and recId to the leaf block at blockNum using the
    // insertIntoLeaf() function.
    // declare a struct Index with attrVal = attrVal, block = recId.block and
    // slot = recId.slot to pass as argument to the function.
    Index entry;
    entry.attrVal = attrVal;
    entry.block = recId.block;
    entry.slot = recId.slot;
    int ret2 = insertIntoLeaf(relId, attrName, leafBlkNum, entry);
    // insertIntoLeaf(relId, attrName, leafBlkNum, Index entry)
    // NOTE: the insertIntoLeaf() function will propagate the insertion to the
    //       required internal nodes by calling the required helper functions
    //       like insertIntoInternal() or createNewRoot()

    if (/*insertIntoLeaf() returns E_DISKFULL */ ret2 == E_DISKFULL)
    {
        // destroy the existing B+ tree by passing the rootBlock to bPlusDestroy().
        bPlusDestroy(blockNum);
        // update the rootBlock of attribute catalog cache entry to -1 using
        attrCatEntry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
        // AttrCacheTable::setAttrCatEntry().

        return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType)
{
    int blockNum = rootBlock;

    while (/*block is not of type IND_LEAF */ StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF)
    { // use StaticBuffer::getStaticBlockType()

        // declare an IndInternal object for block using appropriate constructor
        IndInternal internalBlock(blockNum);

        // get header of the block using BlockBuffer::getHeader()
        HeadInfo head;
        internalBlock.getHeader(&head);

        /* iterate through all the entries, to find the first entry whose
             attribute value >= value to be inserted.
             NOTE: the helper function compareAttrs() declared in BlockBuffer.h
                   can be used to compare two Attribute values. */
        int index = 0;
        int flag = 0;
        InternalEntry intEntry;
        while (index < head.numEntries)
        {
            internalBlock.getEntry(&intEntry, index);
            int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrType);
            if (cmpVal > 0)
            {
                flag = 1;
                break;
            }
            index++;
        }

        if (/*no such entry is found*/ flag == 0)
        {
            // set blockNum = rChild of (nEntries-1)'th entry of the block
            // (i.e. rightmost child of the block)
            blockNum = intEntry.rChild;
        }
        else
        {
            // set blockNum = lChild of the entry that was found
            blockNum = intEntry.lChild;
        }
    }

    return blockNum;
}

/// setheader
int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry)
{
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // declare an IndLeaf instance for the block using appropriate constructor
    IndLeaf leafBlock(blockNum);

    HeadInfo blockHeader;
    // store the header of the leaf index block into blockHeader
    // using BlockBuffer::getHeader()
    leafBlock.getHeader(&blockHeader);

    // the following variable will be used to store a list of index entries with
    // existing indices + the new index to insert
    Index indices[blockHeader.numEntries + 1];

    /*
    Iterate through all the entries in the block and copy them to the array indices.
    Also insert `indexEntry` at appropriate position in the indices array maintaining
    the ascending order.
    - use IndLeaf::getEntry() to get the entry
    - use compareAttrs() declared in BlockBuffer.h to compare two Attribute structs
    */
    int j = 0;
    bool first = true;
    for (int i = 0; i < blockHeader.numEntries; i++)
    {
        Index indentry;
        leafBlock.getEntry(&indentry, i);
        int cmpVal = compareAttrs(indentry.attrVal, indexEntry.attrVal, attrCatEntry.attrType);
        if (cmpVal >= 0 && first)
        {
            indices[j++] = indexEntry;
            first = false;
        }
        indices[j++] = indentry;
    }
    if (first == true)
    {
        indices[j] = indexEntry;
    }

    if (blockHeader.numEntries != MAX_KEYS_LEAF)
    {
        // (leaf block has not reached max limit)

        // increment blockHeader.numEntries and update the header of block
        // using BlockBuffer::setHeader().
        blockHeader.numEntries++;
        leafBlock.setHeader(&blockHeader);

        // iterate through all the entries of the array `indices` and populate the
        // entries of block with them using IndLeaf::setEntry().
        for (int i = 0; i < blockHeader.numEntries; i++)
            leafBlock.setEntry(indices + i, i);

        return SUCCESS;
    }

    // If we reached here, the `indices` array has more than entries than can fit
    // in a single leaf index block. Therefore, we will need to split the entries
    // in `indices` between two leaf blocks. We do this using the splitLeaf() function.
    // This function will return the blockNum of the newly allocated block or
    // E_DISKFULL if there are no more blocks to be allocated.

    // *debug begin
    // 1. print StaticBuffer::getBufferNum(<blockNum>)
    // 2. print printBuffer(-1, StaticBuffer::blocks[<bufferIndex>])
    //* debug end

    int newRightBlk = splitLeaf(blockNum, indices);

    // if splitLeaf() returned E_DISKFULL
    //     return E_DISKFULL
    if (newRightBlk == E_DISKFULL)
    {
        return E_DISKFULL;
    }
    int RET = SUCCESS;
    if (/* the current leaf block was not the root */ blockHeader.pblock != -1)
    { // check pblock in header
      // insert the middle value from `indices` into the parent block using the
      // insertIntoInternal() function. (i.e the last value of the left block)

        // the middle value will be at index 31 (given by constant MIDDLE_INDEX_LEAF)

        // create a struct InternalEntry with attrVal = indices[MIDDLE_INDEX_LEAF].attrVal,
        // lChild = currentBlock, rChild = newRightBlk and pass it as argument to
        // the insertIntoInternalFunction as follows
        InternalEntry intentry;
        intentry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        intentry.lChild = blockNum;
        intentry.rChild = newRightBlk;

        // insertIntoInternal(relId, attrName, parent of current block, new internal entry)
        RET = insertIntoInternal(relId, attrName, blockHeader.pblock, intentry);
    }
    else
    {
        // the current block was the root block and is now split. a new internal index
        // block needs to be allocated and made the root of the tree.
        // To do this, call the createNewRoot() function with the following arguments

        // createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal,
        //               current block, new right block)
        RET = createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlk);
    }

    // if either of the above calls returned an error (E_DISKFULL), then return that
    // else return SUCCESS
    if (RET == E_DISKFULL)
    {
        return E_DISKFULL;
    }
    return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[])
{
    // declare rightBlk, an instance of IndLeaf using constructor 1 to obtain new
    // leaf index block that will be used as the right block in the splitting
    IndLeaf rightBlk;

    // declare leftBlk, an instance of IndLeaf using constructor 2 to read from
    // the existing leaf block
    IndLeaf leftBlk(leafBlockNum);

    int rightBlkNum = /* block num of right blk */ rightBlk.getBlockNum();
    int leftBlkNum = /* block num of left blk */ leftBlk.getBlockNum();

    if (/* newly allocated block has blockNum E_DISKFULL */ rightBlkNum == E_DISKFULL)
    {
        //(failed to obtain a new leaf index block because the disk is full)
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    // get the headers of left block and right block using BlockBuffer::getHeader()
    rightBlk.getHeader(&rightBlkHeader);
    leftBlk.getHeader(&leftBlkHeader);

    // set rightBlkHeader with the following values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32,
    // - pblock = pblock of leftBlk
    // - lblock = leftBlkNum
    // - rblock = rblock of leftBlk
    // and update the header of rightBlk using BlockBuffer::setHeader()
    rightBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2; // 32
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlkHeader.lblock = leftBlkNum;
    rightBlkHeader.rblock = leftBlkHeader.rblock;
    rightBlk.setHeader(&rightBlkHeader);

    // set leftBlkHeader with the following values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32
    // - rblock = rightBlkNum
    // and update the header of leftBlk using BlockBuffer::setHeader() */
    leftBlkHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2; // 32
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.setHeader(&leftBlkHeader);

    // set the first 32 entries of leftBlk = the first 32 entries of indices array
    // and set the first 32 entries of newRightBlk = the next 32 entries of
    // indices array using IndLeaf::setEntry().
    for (int i = 0; i < 32; i++)
    {
        leftBlk.setEntry(indices + i, i);
        rightBlk.setEntry(indices + 32 + i, i);
    }

    return rightBlkNum;
}

////////////////////////////
int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry)
{
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // declare intBlk, an instance of IndInternal using constructor 2 for the block
    // corresponding to intBlockNum
    IndInternal intBlk(intBlockNum);

    HeadInfo blockHeader;
    // load blockHeader with header of intBlk using BlockBuffer::getHeader().
    intBlk.getHeader(&blockHeader);

    // declare internalEntries to store all existing entries + the new entry
    InternalEntry internalEntries[blockHeader.numEntries + 1];

    /*
    Iterate through all the entries in the block and copy them to the array
    `internalEntries`. Insert `indexEntry` at appropriate position in the
    array maintaining the ascending order.
        - use IndInternal::getEntry() to get the entry
        - use compareAttrs() to compare two structs of type Attribute

    Update the lChild of the internalEntry immediately following the newly added
    entry to the rChild of the newly added entry.
    */
    int j = 0;
    bool first = true;
    for (int i = 0; i < blockHeader.numEntries; i++)
    {
        InternalEntry indentry;
        intBlk.getEntry(&indentry, i);
        int cmpVal = compareAttrs(indentry.attrVal, intEntry.attrVal, attrCatEntry.attrType);
        if (cmpVal >= 0 && first)
        {
            internalEntries[j++] = intEntry;
            internalEntries[j].lChild = intEntry.rChild; //////////////////////////////////////////////////////////////
            first = false;
        }
        internalEntries[j++] = indentry;
    }
    if (first == true)
    {
        internalEntries[j] = intEntry;
    }

    if (blockHeader.numEntries != MAX_KEYS_INTERNAL)
    {
        // (internal index block has not reached max limit)

        // increment blockheader.numEntries and update the header of intBlk
        // using BlockBuffer::setHeader().
        blockHeader.numEntries++;
        intBlk.setHeader(&blockHeader);

        // iterate through all entries in internalEntries array and populate the
        // entries of intBlk with them using IndInternal::setEntry().
        for (int i = 0; i < blockHeader.numEntries; i++)
        {
            intBlk.setEntry(&internalEntries[i], i);
        }

        return SUCCESS;
    }

    // If we reached here, the `internalEntries` array has more than entries than
    // can fit in a single internal index block. Therefore, we will need to split
    // the entries in `internalEntries` between two internal index blocks. We do
    // this using the splitInternal() function.
    // This function will return the blockNum of the newly allocated block or
    // E_DISKFULL if there are no more blocks to be allocated.

    int newRightBlk = splitInternal(intBlockNum, internalEntries);

    if (/* splitInternal() returned E_DISKFULL */ newRightBlk == E_DISKFULL)
    {

        // Using bPlusDestroy(), destroy the right subtree, rooted at intEntry.rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree
        bPlusDestroy(intEntry.rChild); //////////////////////////////////      HATIM BRO

        return E_DISKFULL;
    }
    int RET = SUCCESS;
    if (/* the current block was not the root */ blockHeader.pblock != -1)
    { // (check pblock in header)
      // insert the middle value from `internalEntries` into the parent block
      // using the insertIntoInternal() function (recursively).

        // the middle value will be at index 50 (given by constant MIDDLE_INDEX_INTERNAL)

        // create a struct InternalEntry with lChild = current block, rChild = newRightBlk
        // and attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal
        // and pass it as argument to the insertIntoInternalFunction as follows
        InternalEntry intentry;
        intentry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        intentry.lChild = intBlockNum;
        intentry.rChild = newRightBlk;

        // insertIntoInternal(relId, attrName, parent of current block, new internal entry)
        RET = insertIntoInternal(relId, attrName, blockHeader.pblock, intentry);
    }
    else
    {
        // the current block was the root block and is now split. a new internal index
        // block needs to be allocated and made the root of the tree.
        // To do this, call the createNewRoot() function with the following arguments

        // createNewRoot(relId, attrName,
        //               internalEntries[MIDDLE_INDEX_INTERNAL].attrVal,
        //               current block, new right block)
        RET = createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlk);
    }

    // if either of the above calls returned an error (E_DISKFULL), then return that
    // else return SUCCESS
    if (RET == E_DISKFULL)
    {
        return E_DISKFULL;
    }
    return SUCCESS;
}

///
int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[])
{
    // declare rightBlk, an instance of IndInternal using constructor 1 to obtain new
    // internal index block that will be used as the right block in the splitting
    IndInternal rightBlk;

    // declare leftBlk, an instance of IndInternal using constructor 2 to read from
    // the existing internal index block
    IndInternal leftBlk(intBlockNum);

    int rightBlkNum = /* block num of right blk */ rightBlk.getBlockNum();
    int leftBlkNum = /* block num of left blk */ leftBlk.getBlockNum();

    if (/* newly allocated block has blockNum E_DISKFULL */ rightBlkNum == E_DISKFULL)
    {
        //(failed to obtain a new internal index block because the disk is full)
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    // get the headers of left block and right block using BlockBuffer::getHeader()
    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);
    // set rightBlkHeader with the following values
    // - number of entries = (MAX_KEYS_INTERNAL)/2 = 50
    // - pblock = pblock of leftBlk
    // and update the header of rightBlk using BlockBuffer::setHeader()
    rightBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2; // 50
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlk.setHeader(&rightBlkHeader);

    // set leftBlkHeader with the following values
    // - number of entries = (MAX_KEYS_INTERNAL)/2 = 50
    // - rblock = rightBlkNum
    // and update the header using BlockBuffer::setHeader()
    leftBlkHeader.numEntries = (MAX_KEYS_INTERNAL) / 2;
    leftBlkHeader.rblock = rightBlkNum;
    leftBlk.setHeader(&leftBlkHeader);

    /*
    - set the first 50 entries of leftBlk = index 0 to 49 of internalEntries
      array
    - set the first 50 entries of newRightBlk = entries from index 51 to 100
      of internalEntries array using IndInternal::setEntry().
      (index 50 will be moving to the parent internal index block)
    */
    for (int i = 0; i < 50; i++)
    {
        leftBlk.setEntry(&internalEntries[i], i);
        rightBlk.setEntry(&internalEntries[i+51], i);
    }
    int type = StaticBuffer::getStaticBlockType(internalEntries->rChild); /* block type of a child of any entry of the internalEntries array */
    //(use StaticBuffer::getStaticBlockType())

    for (/* each child block of the new right block */ int i = 0; i < rightBlkHeader.numEntries; i++) /////////////////////////////////////////////////////////////////////
    {
        //*CHECK HERE ONCEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE
        InternalEntry intentry;
        rightBlk.getEntry(&intentry, i);
        // declare an instance of BlockBuffer to access the child block using
        // constructor 2
        if (i == 0)
        {
            BlockBuffer BLOcK(intentry.lChild);
            HeadInfo HeAD;
            BLOcK.getHeader(&HeAD);
            HeAD.pblock = rightBlkNum;
            BLOcK.setHeader(&HeAD);
        }
        BlockBuffer BlocK(intentry.rChild);
        HeadInfo HeaD;
        BlocK.getHeader(&HeaD);
        HeaD.pblock = rightBlkNum;
        BlocK.setHeader(&HeaD);
        // update pblock of the block to rightBlkNum using BlockBuffer::getHeader()
        // and BlockBuffer::setHeader().
    }

    return rightBlkNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild)
{
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // declare newRootBlk, an instance of IndInternal using appropriate constructor
    // to allocate a new internal index block on the disk
    IndInternal newRootBlk;

    int newRootBlkNum = /* block number of newRootBlk */ newRootBlk.getBlockNum();

    if (newRootBlkNum == E_DISKFULL)
    {
        // (failed to obtain an empty internal index block because the disk is full)

        // Using bPlusDestroy(), destroy the right subtree, rooted at rChild.
        // This corresponds to the tree built up till now that has not yet been
        // connected to the existing B+ Tree
        BPlusTree::bPlusDestroy(rChild);

        return E_DISKFULL;
    }

    // update the header of the new block with numEntries = 1 using
    // BlockBuffer::getHeader() and BlockBuffer::setHeader()
    HeadInfo HEADER;
    newRootBlk.getHeader(&HEADER);
    HEADER.numEntries = 1;
    newRootBlk.setHeader(&HEADER);

    // create a struct InternalEntry with lChild, attrVal and rChild from the
    // arguments and set it as the first entry in newRootBlk using IndInternal::setEntry()
    InternalEntry intEntry;
    intEntry.lChild = lChild;
    intEntry.attrVal = attrVal;
    intEntry.rChild = rChild;
    newRootBlk.setEntry(&intEntry, 0);

    // declare BlockBuffer instances for the `lChild` and `rChild` blocks using
    // appropriate constructor and update the pblock of those blocks to `newRootBlkNum`
    // using BlockBuffer::getHeader() and BlockBuffer::setHeader()
    BlockBuffer leftChild(lChild);
    BlockBuffer rightChild(rChild);
    HeadInfo LHEAD, RHEAD;
    leftChild.getHeader(&LHEAD);
    rightChild.getHeader(&RHEAD);
    LHEAD.pblock = newRootBlkNum;
    RHEAD.pblock = newRootBlkNum;
    leftChild.setHeader(&LHEAD);
    rightChild.setHeader(&RHEAD);

    // update rootBlock = newRootBlkNum for the entry corresponding to `attrName`
    // in the attribute cache using AttrCacheTable::setAttrCatEntry().
    attrCatEntry.rootBlock = newRootBlkNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    return SUCCESS;
}