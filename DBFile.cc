#include "TwoWayList.h"
#include<iostream>
#include<fstream>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
	currentRecord = new Record;
	currentPage = new Page;
	filePtr = new File;
	curPageNum = 0;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	filePtr->Open(0,f_path);
	//filePtr->Close();
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	//loadpath includes tpch_dir+schema name
	cout<<"loadpath: "<<loadpath<<endl;
	//open file for reading
	FILE * fp = fopen(loadpath,"r");
	int pageCounter = 0;
	while(currentRecord->SuckNextRecord(&f_schema,fp) == 1){
		//if the current page is full, make a new page else append to the currentPage.
		int status = currentPage->Append(currentRecord);
  
 		if(status == 0) {//page full
			filePtr->AddPage(currentPage,pageCounter);//write the page to file
			currentPage->EmptyItOut();
			currentPage->Append(currentRecord);//now add the previous record to the new page
			++pageCounter;
		}
	}
	filePtr->AddPage(currentPage,pageCounter);//write the last page to file: its didnt get full
	
	//set number of pages in DBFile
	numberOfPages = pageCounter;

	//remove the page content: not needed
//	currentPage->EmptyItOut();

	//creating a meta-data file to save the number of pages in DBFile. Other things can be saved to this when required.
	ofstream metaFile("metaData.header");
	if(metaFile.is_open()) {
		metaFile<<pageCounter;
		metaFile.close();
	}


	cout<<"No of pages: "<<pageCounter<<" |" <<filePtr->GetLength()<<endl;

//remove this code
//code to read the DBFile back
/*
	int cnt = 0;	
	for(int i =0;i<=pageCounter;i++)
	{
		filePtr->GetPage(currentPage,i);
		while(currentPage->GetFirst(currentRecord))
			currentRecord->Print(&f_schema);
		++cnt;
	}
	cout<<endl<<cnt<<endl;
*/
}

int DBFile::Open (char *f_path) {
	//need to return 1 for success , 0 for failure.
	//f.Open will give error and exit in case it could not open the file

	//1st parameter: fileLen is 1 because you dont recreate it: just open it.
	filePtr->Open(1,f_path);

	//do I really need this ?? 
	ifstream metaFile("metaData.header");
	if(metaFile.is_open()) {
		metaFile>>numberOfPages;
		metaFile.close();
	}
}

void DBFile::MoveFirst () {
	filePtr->GetPage(currentPage,0);
}

int DBFile::Close () {
	//need to return 1 for success , 0 for failure.
	filePtr->Close();
}

void DBFile::Add (Record &rec) {
	int lastPage = filePtr->GetLength();
	filePtr->GetPage(currentPage,lastPage);
	currentPage->Append(&rec);
	//need to see whether updating the last page automatically updates the file or not.
}

int DBFile::GetNext (Record &fetchme) {
	//check whether you are within page limit of file
	if(curPageNum > filePtr->GetLength())
		return 0;

	if(currentPage->GetFirst(currentRecord) != 1) //if there is no record left on currentPage, read next page from file into currentPage
	{
		++curPageNum;
		filePtr->GetPage(currentPage,curPageNum);
	}

	fetchme = *currentRecord;
	return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//check whether you are within page limit of file
	if(curPageNum > filePtr->GetLength())
		return 0;

	if(currentPage->GetFirst(currentRecord) != 1) //if there is no record left on currentPage, read next page from file into currentPage
	{
		++curPageNum;
		filePtr->GetPage(currentPage,curPageNum);
	}

	fetchme = *currentRecord;


	// print out the comparison to the screen
	cnf.Print ();

	int counter = 0;
	ComparisonEngine comp;
	if (comp.Compare (&fetchme, &literal, &cnf))
		return 1;
	else
		return 0;
}
