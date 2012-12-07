#include "stdio.h"
#include <string.h>
#include "Python.h"
#include "dynamoDBc.h"

PyObject *make_dictionary(int length, char* name, char* lastname, char* city)
{
        PyObject *pDict = PyDict_New();

        PyDict_SetItem(pDict, Py_BuildValue("s", "length"), Py_BuildValue("i", length));
        PyDict_SetItem(pDict, Py_BuildValue("s", "name"), Py_BuildValue("s", name));
        PyDict_SetItem(pDict, Py_BuildValue("s", "lastname"), Py_BuildValue("s", lastname));
        PyDict_SetItem(pDict, Py_BuildValue("s", "city"), Py_BuildValue("s", city));

        return pDict;
}

int main(int argc, char *argv[])
{
	PyObject *pModule, *pConn, *pTable, *pItem;

	init_python("/home/jasper/dynamoDBc");
	pModule = get_module("dynamodb");
	if( pModule == NULL)
	{
		printf("Not able find dynamodb.py module");
		exit(1);
	}
	pConn = get_conn(pModule, "eu-west-1");
	if( pConn == NULL)
	{
		printf("Not to make a dynamoDB connection");
		exit(1);
	}

	pTable = get_table(pModule, pConn, "testingc"); 
	if(pTable == NULL)
	{
		printf("Not to get table");
		exit(1);
	}

	// get an item
	pItem = get_item(pModule, pTable, 5);
	printf("We got the item: %s\n", PyString_AsString(pItem));
	Py_DECREF(pItem);
        Py_DECREF(pTable);
	
	// and add  an item
	pTable = get_table(pModule, pConn, "testingc"); 
	PyObject *pDict = make_dictionary(110, "Jan", "Klaas", "Hoorn");
	set_item(pModule, pTable, 7, pDict); 
	Py_DECREF(pDict);
        Py_DECREF(pTable);

	// and add  an item
	pTable = get_table(pModule, pConn, "testingc"); 
	PyObject *pDictTwo = make_dictionary(120, "Piet", "Klaas", "Enkhuizen");
	set_item(pModule, pTable, 8, pDictTwo); 
	Py_DECREF(pDictTwo);
        Py_DECREF(pTable);

	// and add an other one
	pTable = get_table(pModule, pConn, "testingc");
	PyObject *pDictThree = make_dictionary(130, "Kees", "Klaas", "Medeblik");
	set_item(pModule, pTable, 9, pDictThree); 
	Py_DECREF(pDictThree);
        Py_DECREF(pTable);

	// and add an other one
	pTable = get_table(pModule, pConn, "testingc");
	PyObject *pDictFour = make_dictionary(140, "Joost", "Klaas", "Hem");
	set_item(pModule, pTable, 10, pDictFour); 
	Py_DECREF(pDictFour);
        Py_DECREF(pTable);

	// cleanup objects
	Py_DECREF(pModule);
	Py_DECREF(pConn);

	Py_Finalize();
}

