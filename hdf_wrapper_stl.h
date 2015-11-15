#ifndef HDF_WRAPPER_STL_H
#define HDF_WRAPPER_STL_H

#ifdef HDF_WRAPPER_HAS_BOOST
#include <boost/mpl/bool.hpp>
#include <boost/range/begin.hpp>
#include <boost/range/end.hpp>
#endif

#include "hdf_wrapper.h"


namespace h5cpp
{

/*==================================================
*          some free functions
*===================================================*/
/*--------------------------------------------------
*            dataspaces
* ------------------------------------------------ */

/*
  Arguments specify dimensions. The rank is determined from the first argument that is zero.
  E.g. create_dataspace(5, 6)  and create_dataspace(5, 6, 0, 10) both result in a dataspace
  of rank two. Any arguments following a zero are ignored.
*/
inline Dataspace create_dataspace(hsize_t dim0, hsize_t dim1 = 0, hsize_t dim2 = 0, hsize_t dim3 = 0, hsize_t dim4 = 0, hsize_t dim5 = 0)
{
  enum { MAX_DIM = 6 };
  const hsize_t xa[MAX_DIM] = { (hsize_t)dim0, (hsize_t)dim1, (hsize_t)dim2, (hsize_t)dim3, (hsize_t)dim4, (hsize_t)dim5 };
  int rank = 0;
  for (; rank<MAX_DIM; ++rank) { // find rank
    if (xa[rank] <= 0) break;
  }
  if (rank <= 0)
    throw Exception("nd dataspace with rank 0 not permitted, use create_scalar()");
  return Dataspace::create_simple(rank, xa);
}

template<class iterator>
inline Dataspace create_dataspace_from_iter(iterator begin, iterator end)
{
  hsize_t dims[H5S_MAX_RANK];
  int i = 0;
  for (; begin != end; ++begin)
  {
    dims[i++] = *begin;

    if (i >= H5S_MAX_RANK)
      throw Exception("error creating dataspace: provided range is too large");
  }
  return Dataspace::create_simple(i, dims);
}

#ifdef HDF_WRAPPER_HAS_BOOST
template<class Range>
inline Dataspace create_dataspace_from_range(const Range &dims)
{
  return create_dataspace_from_iter(boost::begin(dims), boost::end(dims));
}
#endif

template<class T, class A>
inline void get_dims(std::vector<T, A> &ret, const Dataspace &sp)
{
  hsize_t dims[H5S_MAX_RANK];
  int r = sp.get_dims(dims);
  ret.resize(r);
  std::copy(dims, dims + r, ret.begin());
}


/*--------------------------------------------------
*            datasets
* ------------------------------------------------ */
template<class T>
inline Dataset create_dataset_simple(Group group, const std::string &name, const Dataspace &sp, const T* data, DsCreationFlags flags = CREATE_DS_DEFAULT)
{
  assert(data != nullptr);
  Dataset ds = Dataset::create(group, name, get_disktype<T>(), sp, Dataset::create_creation_properties(sp, flags));
  ds.write<T>(data);
  return ds;
}

template<class T>
inline Dataset create_dataset_scalar(Group group, const std::string &name, const T& data)
{
  Dataspace sp = Dataspace::create_scalar();
  Dataset ds = Dataset::create(group, name, get_disktype<T>(), sp, Dataset::create_creation_properties(sp, NONE));
  ds.write<T>(&data);
  return ds;
}


template<class T, class A>
inline Dataset create_dataset(Group group, const std::string &name, const std::vector<T, A> &data, DsCreationFlags flags = CREATE_DS_DEFAULT)
{
  return create_dataset_simple(group, name, create_dataspace(data.size()), &data[0], flags);
}


template<class T, class A>
inline void read_dataset(std::vector<T, A> &ret, const Dataset ds)
{
  Dataspace sp = ds.get_dataspace();
  ret.resize(sp.get_npoints());
  ds.read(&ret[0]);
}


/*--------------------------------------------------
 *            Attributes
 * ------------------------------------------------ */

template<class T>
inline void set_attribute(Attributes attrs, const std::string &name, const std::vector<T> &data)
{
  attrs.set(name, create_dataspace(data.size()), &data[0]);
}

template<class T>
inline void set_attribute(Attributes attrs, const std::string &name, const T* data, hsize_t count)
{
  attrs.set(name, create_dataspace(count), data);
}

template<class T, class A>
inline void get_attribute(std::vector<T, A> &ret, Attributes attrs, const std::string &name)
{
  Attribute a = attrs.open(name);
  ret.resize(a.get_dataspace().get_npoints());
  a.read<T>(&ret[0]);
}

}


#endif
