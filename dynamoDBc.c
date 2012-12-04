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
	PyObject *pName, *pModule;
	pModule = NULL;

        pName = PyString_FromString(module);
        pModule = PyImport_Import(pName);
	if (pModule == NULL)
	{
		printf("we do NOT get the module %s\n", module);
	}
	return pModule;
}

PyObject* get_conn(PyObject *pModule, char* region)
{
	PyObject *pArgs, *pRegion;
	PyObject *pFunc, *pConn;

	pConn = NULL;
	pFunc = NULL;

        pFunc = PyObject_GetAttrString(pModule, "connect_dynamodb");
        if (pFunc == NULL || PyCallable_Check(pFunc) == 0)
        {
		printf("Function does not exists: connect_dynamodb");
		return pConn;
	}

        pArgs = PyTuple_New(1);
        pRegion = PyString_FromString(region);
        PyTuple_SetItem(pArgs, 0, pRegion);

        pConn = PyObject_CallObject(pFunc, pArgs);
        if (pConn == NULL)
        {
		printf("Failed to make connection with dynamoDB, are credentials set in env\n");
	}

	// cleanup temp objects
	Py_DECREF(pArgs);
	Py_DECREF(pRegion);
	Py_DECREF(pFunc);

	return pConn;
}

PyObject* get_table(PyObject *pModule, PyObject *pConn, char *table)
{
	PyObject *pFunc;
	PyObject *pArgs, *pTableName;
	PyObject *pTable;

	pFunc = PyObject_GetAttrString(pModule, "get_table");
	pArgs = PyTuple_New(2);

	pTableName = PyString_FromString(table);
	PyTuple_SetItem(pArgs, 0, pConn);
	PyTuple_SetItem(pArgs, 1, pTableName);
	pTable = PyObject_CallObject(pFunc, pArgs);
	if (pTable == NULL)
	{
		printf("Failed to get the table: %sn", table);
	}

	// cleanup temp objects
	// Py_DECREF(pArgs);   // Do not delete pArgs somehow needed by pTable (connected); gives a 'Segmentation fault'
	Py_DECREF(pTableName);
	Py_DECREF(pFunc);

	return pTable;
}

PyObject* get_item(PyObject *pModule, PyObject *pTable, int index)
{
	PyObject *pArgs, *pIndex, *pFunc;  
	PyObject *pItem;

	pFunc = PyObject_GetAttrString(pModule, "get_item");
	pArgs = PyTuple_New(2);
	pIndex = PyInt_FromLong(1);
	PyTuple_SetItem(pArgs, 0, pTable);
	PyTuple_SetItem(pArgs, 1, pIndex);
	pItem = PyObject_CallObject(pFunc, pArgs);
	if (pItem == NULL) 
	{
		printf("Not able to get item %d", index);
	}

	// cleanup temp objects
	Py_DECREF(pArgs);
	Py_DECREF(pIndex);
	Py_DECREF(pFunc);

	return pItem;
}

PyObject *make_dictionary(int length, char* name, char* lastname, char* city)
{
	PyObject *pDict = PyDict_New();

	PyDict_SetItem(pDict, Py_BuildValue("s", "length"), Py_BuildValue("i", length));
	PyDict_SetItem(pDict, Py_BuildValue("s", "name"), Py_BuildValue("s", name));
	PyDict_SetItem(pDict, Py_BuildValue("s", "lastname"), Py_BuildValue("s", lastname));
	PyDict_SetItem(pDict, Py_BuildValue("s", "city"), Py_BuildValue("s", city));

	return pDict;
}

PyObject* set_item(PyObject *pModule, PyObject *pTable, int index, PyObject *pDict)
{
	PyObject *pArgs, *pIndex, *pFunc;
	PyObject *pItem;

	pFunc = PyObject_GetAttrString(pModule, "set_item");
	pArgs = PyTuple_New(3);
	pIndex = PyInt_FromLong(index);
	PyTuple_SetItem(pArgs, 0, pTable);
	PyTuple_SetItem(pArgs, 1, pIndex);
	PyTuple_SetItem(pArgs, 2, pDict);
	pItem = PyObject_CallObject(pFunc, pArgs);
	if (pItem == NULL) 
	{
		printf("Not able to set item %d\n", index);
	}

	// cleanup temp objects
	Py_DECREF(pArgs);
	Py_DECREF(pIndex);
	Py_DECREF(pFunc);

	return pItem;
}

