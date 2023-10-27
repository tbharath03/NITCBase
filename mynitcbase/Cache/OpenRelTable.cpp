#include "OpenRelTable.h"

#include <cstring>
#include <stdlib.h>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {

  // initialize relCache and attrCache with nullptr
  for (int i = 0; i < MAX_OPEN; ++i) {
    RelCacheTable::relCache[i] = nullptr;
    AttrCacheTable::attrCache[i] = nullptr;
    tableMetaInfo[i].free=true;
  }

  /************ Setting up Relation Cache entries ************/
  // (we need to populate relation cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Relation Cache Table****/
  RecBuffer relCatBlock(RELCAT_BLOCK);

  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);

  struct RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

  // allocate this on the heap because we want it to persist outside this function
  RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

  /**** setting up Attribute Catalog relation in the Relation Cache Table ****/
  relCatBlock.getRecord(relCatRecord,RELCAT_SLOTNUM_FOR_ATTRCAT);
  
  RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
  relCacheEntry.recId.block = RELCAT_BLOCK;
  relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;
  
  RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheEntry;
			
	/*	
	HeadInfo relCatHeader;
	relCatBlock.getHeader(&relCatHeader);
	
	for(int i=0;i<relCatHeader.numEntries;i++){

         relCatBlock.getRecord(relCatRecord, i);

        if(!strcmp(relCatRecord[RELCAT_REL_NAME_INDEX].sVal,"Students")){

           RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
           relCacheEntry.recId.block = RELCAT_BLOCK;
           relCacheEntry.recId.slot = i;

          for (int j = 0; j < MAX_OPEN; ++j) {

              if(RelCacheTable::relCache[j] == nullptr){

                    RelCacheTable::relCache[j] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
                    *(RelCacheTable::relCache[j]) = relCacheEntry;
                } 
            }
     	break;
        }
        
    }
    */
  			
  			
  // set up the relation cache entry for the attribute catalog similarly
  // from the record at RELCAT_SLOTNUM_FOR_ATTRCAT

  // set the value at RelCacheTable::relCache[ATTRCAT_RELID]


  /************ Setting up Attribute cache entries ************/
  // (we need to populate attribute cache with entries for the relation catalog
  //  and attribute catalog.)

  /**** setting up Relation Catalog relation in the Attribute Cache Table ****/
  RecBuffer attrCatBlock(ATTRCAT_BLOCK);

  Attribute attrCatRecord[ATTRCAT_NO_ATTRS];

  // iterate through all the attributes of the relation catalog and create a linked
  // list of AttrCacheEntry (slots 0 to 5)
  // for each of the entries, set
  //    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
  //    attrCacheEntry.recId.slot = i   (0 to 5)
  //    and attrCacheEntry.next appropriately
  // NOTE: allocate each entry dynamically using malloc
  
  struct AttrCacheEntry *head=nullptr;
  struct AttrCacheEntry *prev=nullptr;
  for(int i=0;i<ATTRCAT_NO_ATTRS;i++){
  	AttrCacheEntry *attrCacheEntry=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
  	attrCatBlock.getRecord(attrCatRecord,i);
  	AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&attrCacheEntry->attrCatEntry);
  	attrCacheEntry->recId.block=ATTRCAT_BLOCK;
  	attrCacheEntry->recId.slot=i;
  	if(head==nullptr){
  		head=attrCacheEntry;
  		prev=head;
  	}
  	else{
  		prev->next=attrCacheEntry;
  		prev=prev->next;
  	}
  	attrCacheEntry->next=nullptr;
  }
  
  // set the next field in the last entry to nullptr

  AttrCacheTable::attrCache[RELCAT_RELID] = head;
  /**** setting up Attribute Catalog relation in the Attribute Cache Table ****/

  // set up the attributes of the attribute cache similarly.
  // read slots 6-11 from attrCatBlock and initialise recId appropriately
 
  struct AttrCacheEntry *head2=nullptr;
  struct AttrCacheEntry *prev2=nullptr;
  for(int i=6;i<12;i++){
  	AttrCacheEntry *attrCacheEntry=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
  	attrCatBlock.getRecord(attrCatRecord,i);
  	AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&attrCacheEntry->attrCatEntry);
  	attrCacheEntry->recId.block=ATTRCAT_BLOCK;
  	attrCacheEntry->recId.slot=i;
  	if(head2==nullptr){
  		head2=attrCacheEntry;
  		prev2=head2;
  	}
  	else{
  		prev2->next=attrCacheEntry;
  		prev2=prev2->next;
  	}
  	attrCacheEntry->next=nullptr;
  	
  }

  AttrCacheTable::attrCache[ATTRCAT_RELID]=head2;
/*
  struct AttrCacheEntry *head3=nullptr;
  struct AttrCacheEntry *prev3=nullptr;  
  	
    RecBuffer attrCatBuffer(ATTRCAT_BLOCK);
    HeadInfo attrCatHeader;

    attrCatBuffer.getHeader(&attrCatHeader);

    for(int i=0;i<attrCatHeader.numEntries;i++){
        
        attrCatBlock.getRecord(attrCatRecord,i);

       if(!strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal,"Students")){

          AttrCacheEntry *attrCacheEntry=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));

          AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);

          attrCacheEntry->recId.block = ATTRCAT_BLOCK;
          attrCacheEntry->recId.slot = i;
          attrCacheEntry->next = nullptr;
        
          if(head3==nullptr){
            head3=attrCacheEntry;
            prev3=head3;
          }
          else{
              prev3->next=attrCacheEntry;
              prev3=prev3->next;
          }

          
       }
    }
    for (int i = 0; i < MAX_OPEN; ++i) {
         if(AttrCacheTable::attrCache[i] == nullptr){

             AttrCacheTable::attrCache[i] = head3;
             i=MAX_OPEN;

           } 
      }
 */
 tableMetaInfo[RELCAT_RELID].free=false;
 tableMetaInfo[ATTRCAT_RELID].free=false;
 strcpy(tableMetaInfo[RELCAT_RELID].relName,"RELATIONCAT");
 strcpy(tableMetaInfo[ATTRCAT_RELID].relName,"ATTRIBUTECAT"); 
}

/* This function will open a relation having name `relName`.
Since we are currently only working with the relation and attribute catalog, we
will just hardcode it. In subsequent stages, we will loop through all the relations
and open the appropriate one.
*/
int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  for(int i=0;i<MAX_OPEN;i++){
  	if(tableMetaInfo[i].free==false && !strcmp(relName,tableMetaInfo[i].relName)){            //// change made by me
  		return i;
  	}
  }

  return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/
    for(int i=2;i<MAX_OPEN;i++){
    	if(tableMetaInfo[i].free){
    		return i;
    	}
    }
	return E_CACHEFULL;
  // if found return the relation id, else return E_CACHEFULL.
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) {
  int ret=OpenRelTable::getRelId(relName);
  if(ret!=E_RELNOTOPEN){
    // (checked using OpenRelTable::getRelId())
	return ret;
    // return that relation id;
  }

  /* find a free slot in the Open Relation Table
     using OpenRelTable::getFreeOpenRelTableEntry(). */
  int freeEntry=OpenRelTable::getFreeOpenRelTableEntry();

  if (freeEntry==E_CACHEFULL){
    return E_CACHEFULL;
  }

  // let relId be used to store the free slot.
  int relId=freeEntry;

  /****** Setting up Relation Cache entry for the relation ******/

  /* search for the entry with relation name, relName, in the Relation Catalog using
      BlockAccess::linearSearch().
      Care should be taken to reset the searchIndex of the relation RELCAT_RELID
      before calling linearSearch().*/
  RelCacheTable:: resetSearchIndex(RELCAT_RELID);
  Attribute attrVal;
  strcpy(attrVal.sVal,relName);
  char attrName[ATTR_SIZE]={"RelName"};

  // relcatRecId stores the rec-id of the relation `relName` in the Relation Catalog.
  RecId relcatRecId=BlockAccess::linearSearch(RELCAT_RELID,attrName,attrVal,0);

  if (relcatRecId.block==-1 && relcatRecId.slot==-1) {
    // (the relation is not found in the Relation Catalog.)
    return E_RELNOTEXIST;
  }
  
  /* read the record entry corresponding to relcatRecId and create a relCacheEntry
      on it using RecBuffer::getRecord() and RelCacheTable::recordToRelCatEntry().
      update the recId field of this Relation Cache entry to relcatRecId.
      use the Relation Cache entry to set the relId-th entry of the RelCacheTable.
    NOTE: make sure to allocate memory for the RelCacheEntry using malloc()
  */
  RecBuffer relCatBuffer(RELCAT_BLOCK);
  Attribute relCatRecord[RELCAT_NO_ATTRS];
  relCatBuffer.getRecord(relCatRecord,relcatRecId.slot);
  
  RelCacheEntry relCacheEntry;
  RelCacheTable::recordToRelCatEntry(relCatRecord,&relCacheEntry.relCatEntry);
  //relCacheEntry.dirty=false;
  relCacheEntry.recId=relcatRecId;
  RelCacheTable::relCache[relId] = (struct RelCacheEntry*)malloc(sizeof(RelCacheEntry));
  *(RelCacheTable::relCache[relId]) = relCacheEntry;
  /****** Setting up Attribute Cache entry for the relation ******/

  // let listHead be used to hold the head of the linked list of attrCache entries.
  AttrCacheEntry* listHead=nullptr;
  AttrCacheEntry* prev=nullptr;

  /*iterate over all the entries in the Attribute Catalog corresponding to each
  attribute of the relation relName by multiple calls of BlockAccess::linearSearch()
  care should be taken to reset the searchIndex of the relation, ATTRCAT_RELID,
  corresponding to Attribute Catalog before the first call to linearSearch().*/
  RelCacheTable:: resetSearchIndex(ATTRCAT_RELID);
  while(true)
  {
      /* let attrcatRecId store a valid record id an entry of the relation, relName,
      in the Attribute Catalog.*/
      RecId attrcatRecId=BlockAccess::linearSearch(ATTRCAT_RELID,attrName,attrVal,0);
      if(attrcatRecId.block != -1 && attrcatRecId.slot != -1){
		RecBuffer attrCatBuffer(attrcatRecId.block);
		HeadInfo attrCatHeader;
		attrCatBuffer.getHeader(&attrCatHeader);
		Attribute attrCatRecord[attrCatHeader.numAttrs];
		attrCatBuffer.getRecord(attrCatRecord,attrcatRecId.slot);
		
		AttrCacheEntry *attrCacheEntry=(AttrCacheEntry*)malloc(sizeof(AttrCacheEntry));
		AttrCacheTable::recordToAttrCatEntry(attrCatRecord,&attrCacheEntry->attrCatEntry);
		attrCacheEntry->recId=attrcatRecId;
		if(listHead==nullptr){
			listHead=attrCacheEntry;
			prev=listHead;
		}
		else{
			prev->next=attrCacheEntry;
			prev=prev->next;
		}
		attrCacheEntry->next=nullptr;
		
      } 
      else{
    	  // (all records over)
     	 break;
      }

      /* read the record entry corresponding to attrcatRecId and create an
      Attribute Cache entry on it using RecBuffer::getRecord() and
      AttrCacheTable::recordToAttrCatEntry().
      update the recId field of this Attribute Cache entry to attrcatRecId.
      add the Attribute Cache entry to the linked list of listHead .*/
      // NOTE: make sure to allocate memory for the AttrCacheEntry using malloc()
  }

  // set the relIdth entry of the AttrCacheTable to listHead.
  AttrCacheTable::attrCache[relId]=listHead;

  /****** Setting up metadata in the Open Relation Table for the relation******/

  // update the relIdth entry of the tableMetaInfo with free as false and
  // relName as the input.
  OpenRelTable::tableMetaInfo[relId].free=false;
  strcpy(OpenRelTable::tableMetaInfo[relId].relName,relName);

  return relId;
}

int OpenRelTable::closeRel(int relId) {
  if (relId==RELCAT_RELID || relId==ATTRCAT_RELID) {
    return E_NOTPERMITTED;
  }

  if (relId<0 || relId>=MAX_OPEN) {
    return E_OUTOFBOUND;
  }

  if (tableMetaInfo[relId].free) {
    return E_RELNOTOPEN;
  }

  // free the memory allocated in the relation and attribute caches which was        /////////////////////
  // allocated in the OpenRelTable::openRel() function

  // update `tableMetaInfo` to set `relId` as a free slot
  // update `relCache` and `attrCache` to set the entry at `relId` to nullptr
  if(RelCacheTable::relCache[relId]->dirty){
  
  	RelCatEntry relCatEntry= RelCacheTable::relCache[relId]->relCatEntry;
  	Attribute relCatRecord[RELCAT_NO_ATTRS];
  	RelCacheTable::relCatEntryToRecord(&relCatEntry,relCatRecord);
  	
  	RecBuffer relCatBlock(RelCacheTable::relCache[relId]->recId.block);
  	
  	relCatBlock.setRecord(relCatRecord,RelCacheTable::relCache[relId]->recId.slot);
  	/*
  	Attribute relCatRecord[RELCAT_NO_ATTRS];
  	RelCatEntry relCatBuf;
  	int res=RelCacheTable::getRelCatEntry(relId,&relCatBuf);
  	RelCacheTable::relCatEntryToRecord(&relCatBuf,relCatRecord);
  	RecId recId=RelCacheTable::relCache[relId]->recId;
  	RecBuffer relCatBlock(recId.block);
  	relCatBlock.setRecord(relCatRecord,recId.slot);
  	*/
  }
  OpenRelTable::tableMetaInfo[relId].free=true;
  RelCacheTable::relCache[relId]=nullptr;
  AttrCacheTable::attrCache[relId]=nullptr;

  return SUCCESS;
}


OpenRelTable::~OpenRelTable() {
/*
    for i from 2 to MAX_OPEN-1:
    {
        if ith relation is still open:
        {
            // close the relation using openRelTable::closeRel().
        }
    }
    */
    for(int i=2;i<MAX_OPEN;i++){
		if(!tableMetaInfo[i].free){
			OpenRelTable::closeRel(i);
		}
	}

    /**** Closing the catalog relations in the relation cache ****/

    //releasing the relation cache entry of the attribute catalog

    if (/* RelCatEntry of the ATTRCAT_RELID-th RelCacheEntry has been modified */RelCacheTable::relCache[ATTRCAT_RELID]->dirty) {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(ATTRCAT_RELID,&relCatEntry);
        Attribute relCatRecord[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&relCatEntry,relCatRecord);
		RecId recId=RelCacheTable::relCache[ATTRCAT_RELID]->recId;
        // declaring an object of RecBuffer class to write back to the buffer
        RecBuffer relCatBlock(recId.block);

        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
        relCatBlock.setRecord(relCatRecord,recId.slot);
    }
    // free the memory dynamically allocated to this RelCacheEntry
	RelCacheTable::relCache[ATTRCAT_RELID]=nullptr;

    //releasing the relation cache entry of the relation catalog

    if(/* RelCatEntry of the RELCAT_RELID-th RelCacheEntry has been modified */RelCacheTable::relCache[RELCAT_RELID]->dirty) {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(RELCAT_RELID,&relCatEntry);
        Attribute relCatRecord[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&relCatEntry,relCatRecord);
		RecId recId=RelCacheTable::relCache[RELCAT_RELID]->recId;

        // declaring an object of RecBuffer class to write back to the buffer
        RecBuffer relCatBlock(recId.block);

        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
        relCatBlock.setRecord(relCatRecord,recId.slot);
    }
    // free the memory dynamically allocated for this RelCacheEntry
	RelCacheTable::relCache[RELCAT_RELID]=nullptr;

    // free the memory allocated for the attribute cache entries of the
    // relation catalog and the attribute catalog
    AttrCacheTable::attrCache[RELCAT_RELID]=nullptr;
    AttrCacheTable::attrCache[ATTRCAT_RELID]=nullptr;
}
