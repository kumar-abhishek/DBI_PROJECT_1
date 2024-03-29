
#include <iostream>
#include "Record.h"
#include <stdlib.h>
using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main () {

	// try to parse the CNF
	cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file
	Schema lineitem ("catalog", "lineitem");

	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();

	// now open up the text file and start procesing it
        FILE *tableFile = fopen ("./lineitem.tbl", "r");

        Record temp;
        Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

/* 
        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in
	int counter = 0;
	ComparisonEngine comp;
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		if (comp.Compare (&temp, &literal, &myComparison))
                	temp.Print (&mySchema);

			}
			*/

	//create a Page
	Page p;

	//load up a page full of records 
	int cnt = 0;
	while(temp.SuckNextRecord(&mySchema, tableFile) == 1){
		++cnt;
		p.Append(&temp);
	}
	cout<<cnt<<endl;

	//write the page to file.
	File f;
	f.Open(0,"pageDump");
	f.AddPage(&p,0);
	f.Close();
	p.EmptyItOut();


	//try to retrieve the page from file 
	f.Open(1,"pageDump");
	f.GetPage(&p,0);
		

	//and print the records to the screen using page.
	while(p.GetFirst(&temp))
		temp.Print(&mySchema);
		
}


