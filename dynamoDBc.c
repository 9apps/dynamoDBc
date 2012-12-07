#include "stdio.h"
#include <string.h>
#include "Python.h"
#include "dynamoDBc.h"

void init_python(char *module_dir)
{
	char *runString;

	Py_Initialize();
	PyRun_SimpleString("import sys");
	asprintf(&runString, "sys.path.append(\"%s\")", module_dir);
        PyRun_SimpleString(runString);

	free(runString);
}

PyObject* get_module(char *module)
{
	PyObject *pName = NULL, *pModule = NULL;

        pName = PyString_FromString(module);
        pModule = PyImport_Import(pName);
	if (pModule == NULL)
	{
		printf("failed to get the moduel%s\n", module);
	}

	Py_DECREF(pName);
	return pModule;
}

PyObject* get_conn(PyObject *pModule, char* region)
{
	PyObject *pArgs = NULL;
	PyObject *pFunc = NULL, *pConn = NULL;

        pFunc = PyObject_GetAttrString(pModule, "connect_dynamodb");
        if (pFunc == NULL || PyCallable_Check(pFunc) == 0)
        {
		printf("Function does not exists: connect_dynamodb");
		return pConn;
	}

        pArgs = PyTuple_New(1);
        PyTuple_SetItem(pArgs, 0, PyString_FromString(region));

        pConn = PyObject_CallObject(pFunc, pArgs);
        if (pConn == NULL)
        {
		printf("Failed to make connection with dynamoDB, are credentials set in env\n");
	}

	// cleanup temp objects
	Py_DECREF(pArgs);
	Py_DECREF(pFunc);

	return pConn;
}

PyObject* get_table(PyObject *pModule, PyObject *pConn, char *table)
{
	PyObject *pFunc = NULL;
	PyObject *pARGS = NULL;
	PyObject *pTable = NULL;  // return value

	pFunc = PyObject_GetAttrString(pModule, "get_table");
	pARGS = PyTuple_New(2);

	Py_INCREF(pConn);         // so pConn ref is not deleted by Py_XDECREF(pARGS)
	PyTuple_SetItem(pARGS, 0, pConn);
	PyTuple_SetItem(pARGS, 1, PyString_FromString(table));
	pTable = PyObject_CallObject(pFunc, pARGS);
	if (pTable == NULL)
	{
		printf("Failed to get the table: %sn", table);
	}

	// cleanup temp objects
	Py_XDECREF(pARGS); 
	Py_XDECREF(pFunc);

	return pTable;
}

PyObject* get_item(PyObject *pModule, PyObject *pTable, int index)
{
	PyObject *pArgs = NULL, *pFunc = NULL;  
	PyObject *pItem = NULL;

	pFunc = PyObject_GetAttrString(pModule, "get_item");
	pArgs = PyTuple_New(2);
	PyTuple_SetItem(pArgs, 0, pTable);
	PyTuple_SetItem(pArgs, 1, PyInt_FromLong(index));
	pItem = PyObject_CallObject(pFunc, pArgs);
	if (pItem == NULL) 
	{
		printf("Not able to get item %d", index);
	}

	// cleanup temp objects
	Py_DECREF(pArgs);
	Py_DECREF(pFunc);

	return pItem;
}

PyObject* set_item(PyObject *pModule, PyObject *pTable, int index, PyObject *pDict)
{
	PyObject *pArgs = NULL, *pFunc = NULL;
	PyObject *pItem = NULL;

	pFunc = PyObject_GetAttrString(pModule, "set_item");
	pArgs = PyTuple_New(3);
	PyTuple_SetItem(pArgs, 0, pTable);
	PyTuple_SetItem(pArgs, 1, PyInt_FromLong(index));
	PyTuple_SetItem(pArgs, 2, pDict);
	pItem = PyObject_CallObject(pFunc, pArgs);
	if (pItem == NULL) 
	{
		printf("Not able to set item %d\n", index);
	}

	// cleanup temp objects
	Py_DECREF(pArgs);
	Py_DECREF(pFunc);

	return pItem;
}

