#ifndef HDF_WRAPPER_STL_H
#define HDF_WRAPPER_STL_H

#include <boost/mpl/bool.hpp>
#include <boost/range/begin.hpp>
#include <boost/range/end.hpp>

#include "hdf_wrapper.h"

namespace my
{
  
namespace h5
{

enum { contiguous_mem = 1 };

template<class Range>
inline Dataspace create_dataspace_from_range(const Range &dims)
{
  std::vector<hsize_t> x;
  std::copy(dims.begin(), dims.end(), std::back_insert_iterator<std::vector<hsize_t> >(x));
  return Dataspace::create_nd(&x[0], x.size());
}


template<class const_iterator>
inline Dataset create_dataset_stl(Group group, const std::string &name, const_iterator begin, const const_iterator end, bool contiguous_mem_ = false)
{
  typedef typename std::iterator_traits<const_iterator>::value_type value_type;
  if (contiguous_mem_)
  {
    size_t size = std::distance(begin, end);
    const value_type* data = &(*begin);
    return Dataset::create_simple(group, name, create_dataspace((int)size), data);
  }
  else
  {
    std::vector<value_type> buffer;
    buffer.reserve(128);
    std::copy(begin, end, std::back_inserter(buffer));
    return create_dataset_stl(group, name, buffer.begin(), buffer.end(), contiguous_mem);
  }
}


template<class iterator>
inline void read_dataset_stl(const Dataset ds, iterator begin, iterator end, bool contiguous_mem_ = false)
{
  typedef typename std::iterator_traits<iterator>::value_type value_type;
  if (contiguous_mem_)
  {
    size_t size = std::distance(begin, end);
    value_type* data = &(*begin);
    Dataspace sp = ds.get_dataspace();
    if (sp.get_count() != size)
      throw Exception("buffer size mismatch");
    ds.read_simple(data);
  }
  else
  {
    size_t cnt =  ds.get_dataspace().get_count();
    std::vector<value_type> buffer(cnt);
    ds.read_simple(&buffer[0]);

    std::copy(buffer.begin(), buffer.end(), begin);
  }
}


template<class T>
inline Dataset create_dataset_range(Group group, const std::string &name, const std::vector<T> &range)
{
  return create_dataset_stl(group, name, boost::begin(range), boost::end(range), contiguous_mem);
}

template<class Range>
inline Dataset create_dataset_range(Group group, const std::string &name, const Range &range, bool contiguous_mem_ = false)
{
  return create_dataset_stl(group, name, boost::begin(range), boost::end(range), contiguous_mem_);
}




template<class Container>
inline void read_dataset_stl_resizing(const Dataset ds, Container &c)
{
  c.resize(ds.get_dataspace().get_count());
  read_dataset_stl(ds, c.begin(), c.end());
}



/*--------------------------------------------------
 *            Attributes
 * ------------------------------------------------ */
template<class const_iterator>
inline void set_array_attribute_stl(Attributes attrs, const std::string &name, const_iterator begin, const const_iterator end, bool contiguous_mem_ = false)
{
  typedef typename std::iterator_traits<const_iterator>::value_type value_type;
  if (contiguous_mem_)
  {
    attrs.set_array<>(name, create_dataspace(std::distance(begin, end)), &(*begin));
  }
  else
  {
    std::vector<value_type> buffer;
    buffer.reserve(128);
    std::copy(begin, end, std::back_inserter(buffer));
    set_array_attribute_stl(attrs, name, buffer.begin(), buffer.end(), contiguous_mem);
  }
}


template<class T>
inline void set_array_attribute_range(Group group, const std::string &name, const std::vector<T> &range)
{
  set_array_attribute_stl(group, name, boost::begin(range), boost::end(range), contiguous_mem);
}


template<class Range>
inline void set_array_attribute_range(Attributes attrs, const std::string &name, const Range &range, bool contiguous_mem_ = false)
{
  set_array_attribute_stl(attrs, name, boost::begin(range), boost::end(range), contiguous_mem_);
}


}

}

#endif
