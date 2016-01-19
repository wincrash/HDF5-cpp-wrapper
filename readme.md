HDF5 C++ Wrapper
========================================

This is a simple, thin, single-header c++ wrapper library around HDF5. It is published under the MIT license in the hope that it might be useful for somebody.

All components of this library reside in the `h5cpp` namespace. There are classes File, Group, Dataset, Dataspace and Datatype, corresponding to HDF5 objects. Drawing inspiration from h5py, there is also an Attributes class. Resources are managed via the references counting mechanism of the HDF5 library. C++ types can be mapped to HDF5 data types automatically with the help of template specializations. Mappings for primitive c-types are predefined. The byte order of data on disk defaults to little endianness.

Documentation can be found here http://dawelter.github.io/hdfcppwrapper/index.html

Note:
* The API might change a little in future, but the library is stable enough for actual use in the author's personal projects.
* The wrapping is incomplete, but you can always use get_id() to obtain the HDF5 identifier.
* The library is not thread safe due to the caching of Datatype instances in local static variables. Also no consideration of MPI compatiblity was done.
* No documentation, but a little demo program used for testing, too.

Tested under:
* MS VC 2010  Win64
* MS VC 2013  Win64
* GCC 4.8.4 Unbuntu 14 x86 64 bit.