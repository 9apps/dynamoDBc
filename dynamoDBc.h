#include "stdio.h"
#include <string.h>
#include "Python.h"

void init_python();
PyObject* get_module(char *module);
PyObject* get_conn(PyObject *pModule, char* region);
PyObject* get_table(PyObject *pModule, PyObject *pConn, char *table);
PyObject* get_item(PyObject *pModule, PyObject *pTable, int index);
PyObject* set_item(PyObject *pModule, PyObject *pTable, int index, PyObject *pDict);
