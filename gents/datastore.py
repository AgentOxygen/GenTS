#!/usr/bin/env python
"""
datastore.py

Developer: Cameron Cummins
Contact: cameron.cummins@utexas.edu
Last Header Update: 04/08/26
"""
import netCDF4 as nc


class GenTSDataStore:
    def __init__(self, *args, **kwargs):
        self._ds = nc.Dataset(*args, **kwargs)
 
    def __getattr__(self, name):
        return getattr(self._ds, name)
 
    def __enter__(self):
        return self
 
    def __exit__(self, *exc):
        self._ds.close()
        return False
 
    def __getitem__(self, key):
        return self._ds[key]
 
    def __setitem__(self, key, value):
        self._ds[key] = value
 
    def __contains__(self, item):
        return item in self._ds
 
    def __len__(self):
        return len(self._ds)
 
    def __repr__(self):
        return f"GenTS Dataset wrapping {self._ds}"