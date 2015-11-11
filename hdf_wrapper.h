#ifndef _HDF_WRAPPER_H
#define _HDF_WRAPPER_H

#include <exception>
#include <iostream> // for dealing with strings in exceptions, mostly
#include <string>// for dealing with strings in exceptions, mostly
#include <sstream>// for dealing with strings in exceptions, mostly
#include <type_traits> // for removal of const qualifiers

#include <assert.h>
#include <vector>
#include <iterator>

#if (defined __APPLE__)
      // implement nice exception messages that need string manipulation
#elif (defined _MSC_VER)
  #include <stdio.h> // for FILE stream manipulation
#elif (defined __GNUG__)
  #include <stdio.h>
#endif

#include "hdf5.h"

#ifdef HDF_WRAPPER_HAS_BOOST
  #include <boost/optional.hpp>
#endif



namespace my
{

namespace h5
{

class Object;
class Dataspace;
class Dataset;
class Datatype;
class Attributes;
class Attribute;
class File;
class Group;


namespace internal
{

struct Tag {};
struct TagOpen {};
struct TagCreate {};

}

static void disableAutoErrorReporting()
{
  H5Eset_auto(H5E_DEFAULT, NULL, NULL);
};

class AutoErrorReportingGuard
{
  void *client_data;
  H5E_auto2_t func;
public:
  AutoErrorReportingGuard()
  {
    H5Eget_auto2(H5E_DEFAULT, &func, &client_data);
  }
  void disableReporting()
  {
  H5Eset_auto2(H5E_DEFAULT, NULL, NULL);
  }
  ~AutoErrorReportingGuard()
  {
    H5Eset_auto2(H5E_DEFAULT, func, client_data);
  }
};


/*
  hack around a strange issue: err_desc is partially filled with garbage (func_name, file_name, desc). Therefore,
  this custom error printer is used.
*/
static herr_t custom_print_cb(unsigned n, const H5E_error2_t *err_desc, void* client_data)
{
  const int MSG_SIZE = 256;
  auto *out = static_cast<std::string*>(client_data);
  char                maj[MSG_SIZE];
  char                min[MSG_SIZE];
  char                cls[MSG_SIZE];
  char             buffer[MSG_SIZE];

  /* Get descriptions for the major and minor error numbers */
  if (H5Eget_class_name(err_desc->cls_id, cls, MSG_SIZE) < 0)
    return -1;

  if (H5Eget_msg(err_desc->maj_num, NULL, maj, MSG_SIZE)<0)
    return -1;

  if (H5Eget_msg(err_desc->min_num, NULL, min, MSG_SIZE)<0)
    return -1;

#if (defined __APPLE__) // not even tested to compile this
  snprintf(buffer, MSG_SIZE, "  () Class: %s, Major: %s, Minor: %s", cls, maj, min);    
#elif (defined _MSC_VER)
  sprintf_s(buffer, MSG_SIZE, "  () Class: %s, Major: %s, Minor: %s", cls, maj, min);
#elif (defined __GNUG__)
  snprintf(buffer, MSG_SIZE, "  () Class: %s, Major: %s, Minor: %s", cls, maj, min);
#endif
  buffer[MSG_SIZE - 1] = 0; // its the only way to be sure ...
  try
  {
    out->append(buffer);
  }
  catch (...)
  {
    // suppress all exceptions since thsi function is called from a c library.
  }
  return 0;
}


class Exception : public std::exception
{
    std::string msg;
  public:
    Exception(const std::string &msg_) : msg(msg_)
    {
#if (defined __APPLE__)
      // implement nice exception messages that need string manipulation
#elif (defined _MSC_VER) || (defined __GNUG__)
      msg.append(". Error Stack:");
      H5Ewalk2(H5E_DEFAULT, H5E_WALK_DOWNWARD, &custom_print_cb, &msg);
#endif
    }
    Exception() : msg("Unspecified error") { assert(false); }
    ~Exception() throw() {}
    const char* what() const throw() { return msg.c_str(); }
};

class NameLookupError : public Exception
{
  public:
    NameLookupError(const std::string &name) : Exception("Cannot find '"+name+"'") {}
};



// definitions are at the end of the file
template<class T>
inline Datatype get_disktype();

template<class T>
inline Datatype get_memtype();


class Object
{
  public:
    Object(const Object &o) : id(o.id) 
    {
      inc_ref();
    }

    virtual ~Object() 
    {
      dec_ref();
    }
    
    Object& operator=(const Object &o)
    {
      if (id == o.id) return *this;
      dec_ref();
      id = o.id;
      inc_ref();
      return *this;
    }
  
    hid_t get_id() const
    {
      return id;
    }

    Object() : id(-1) {}

    // normally we get a new handle. Its cout needs only decreasing.
    Object(hid_t id) : id(id)
    {
      htri_t ok = H5Iis_valid(id);
      if (!ok)
        throw Exception("initialization of Object with invalid handle");
    }
    
    std::string get_name() const
    {
      std::string res;
      ssize_t l = H5Iget_name(id, NULL, 0);
      if (l < 0)
        throw Exception("cannot get object name");
      res.resize(l);
      if (l>0)
        H5Iget_name(id, &res[0], l+1);
      return res;
    }

    std::string get_file_name() const
    {
      std::string res;
      ssize_t l = H5Fget_name(id, NULL, 0);
      if (l < 0)
        throw Exception("cannot get object file name");
      res.resize(l);
      if (l>0)
        H5Fget_name(id, &res[0], l+1);
      return res;
    }

    inline File get_file() const;

    bool is_valid() const
    {
      return H5Iis_valid(id) > 0;
    }

    // references the same object as other
    bool is_same(const Object &other) const
    {
      // just need to compare id's
      return other.id == this->id;
    }

protected:
    void inc_ref()
    {
      if (id < 0) return;
      int r = H5Iinc_ref(id);
      if (r < 0)
        throw Exception("cannot inc ref count");
    }

    void dec_ref()
    {
      if (id < 0) return;
      int r = H5Idec_ref(id);
      if (r < 0)
        throw Exception("cannot dec ref count");
      if (H5Iis_valid(id) > 0)
        id = -1;
    }

		hid_t id;
};

enum DatatypeSelect {
  ON_DISK,
  IN_MEM
};


class Datatype : public Object
{
  private:
    friend class Attribute;
    struct ConsFromPreset {};
    Datatype(hid_t id, ConsFromPreset) : Object() 
    {  
      this->id = H5Tcopy(id);
      if (this->id < 0)
        throw Exception("error initializing from prescribed data type");
    }
    Datatype(hid_t id) : Object(id) {}
  public: 

    Datatype() : Object() {}
    
    template<class T>
    static Datatype createPod(DatatypeSelect)
    {
      //static_assert(false, "specialize template<class T> Datatype as_h5_datatype(DatatypeSelect) for this type");
      assert(false);
      throw Exception("specialize template<class T> Datatype as_h5_datatype(DatatypeSelect) for this type");
    }
    static Datatype createFixedLenString(DatatypeSelect)
    {
      return Datatype(H5T_C_S1, ConsFromPreset());
    }

    static Datatype createArray(const Datatype &base, int ndims, int *dims)
    {
      hsize_t hdims[H5S_MAX_RANK];
      for (int i=0; i<ndims; ++i) hdims[i] = dims[i];
      hid_t id = H5Tarray_create2(base.get_id(), ndims, hdims);
      if (id < 0)
        throw Exception("error creating array data type");
      return Datatype(id);
    }
    
    void set_size(size_t s) 
    {
      herr_t err = H5Tset_size(this->id, s);
      if (err < 0)
        throw Exception("cannot set datatype size");
    }

    void set_variable_size() { set_size(H5T_VARIABLE); }
    
    size_t get_size()  // in bytes
    {
      size_t s = H5Tget_size(this->id);
      if (s == 0)
        throw Exception("cannot get datatype size");
      return s;
    }

    bool is_equal(const Datatype &other) const
    {
      htri_t res = H5Tequal(id, other.get_id());
      if (res < 0)
        throw Exception("cannot compare datatypes");
      return res != 0;
    }
};

#define HDF5_WRAPPER_SPECIALIZE_TYPE(t, tid, dtid) \
  template<> inline Datatype Datatype::createPod<t>(DatatypeSelect sel) { return Datatype(sel == ON_DISK ? dtid : tid, ConsFromPreset()); }\

HDF5_WRAPPER_SPECIALIZE_TYPE(int, H5T_NATIVE_INT, H5T_STD_I32LE)
HDF5_WRAPPER_SPECIALIZE_TYPE(unsigned int, H5T_NATIVE_UINT, H5T_STD_U32LE)
HDF5_WRAPPER_SPECIALIZE_TYPE(unsigned long long, H5T_NATIVE_ULLONG, H5T_STD_U64LE)
HDF5_WRAPPER_SPECIALIZE_TYPE(long long, H5T_NATIVE_LLONG, H5T_STD_I64LE)
HDF5_WRAPPER_SPECIALIZE_TYPE(char, H5T_NATIVE_CHAR, H5T_STD_I8LE)
HDF5_WRAPPER_SPECIALIZE_TYPE(unsigned char, H5T_NATIVE_UCHAR, H5T_STD_U8LE)
HDF5_WRAPPER_SPECIALIZE_TYPE(float, H5T_NATIVE_FLOAT, H5T_IEEE_F32LE)
HDF5_WRAPPER_SPECIALIZE_TYPE(double, H5T_NATIVE_DOUBLE, H5T_IEEE_F64LE)
HDF5_WRAPPER_SPECIALIZE_TYPE(bool, H5T_NATIVE_CHAR, H5T_STD_U8LE)
HDF5_WRAPPER_SPECIALIZE_TYPE(unsigned long, H5T_NATIVE_ULONG, H5T_STD_U64LE)
HDF5_WRAPPER_SPECIALIZE_TYPE(long, H5T_NATIVE_LONG, H5T_STD_I64LE)

template<> inline Datatype Datatype::createPod<const char *>(DatatypeSelect sel)
{
  Datatype dt(H5T_C_S1, ConsFromPreset());
  dt.set_variable_size();
  return dt;
}

template<> inline Datatype Datatype::createPod<char*>(DatatypeSelect sel)
{
  return createPod<const char*>(sel);
}

template<> inline Datatype Datatype::createPod<std::string>(DatatypeSelect sel)
{
  return createPod<const char*>(sel);
}


/*
RW abstracts away how datasets and attributes are written since both works
the same way, afik, except for function names, e.g. H5Awrite vs H5Dwrite.
*/
class RW
{
  public:
    virtual void write(const Datatype &dt, const void* buf) = 0;
    virtual void read(const Datatype &dt, void* buf) = 0;
    virtual Datatype get_datatype() const = 0;
    virtual ~RW() {}
};




template<class T>
struct h5traits_of;



class Dataspace : public Object
{
    friend class Dataset;
    friend class Attributes;
    friend class Attribute;
  private:
    Dataspace(hid_t id, internal::Tag) : Object(id) {}
  
  public:
    static Dataspace create_nd(const hsize_t* dims, const hsize_t ndims)
    {
      hid_t id = H5Screate_simple(ndims, dims, NULL);
      if (id < 0)
      {
        std::ostringstream oss;
        oss << "error creating dataspace with rank " << ndims << " and sizes ";
        for (int i=0; i<ndims; ++i)
          oss << dims[i] << ",";
        throw Exception(oss.str());
      }
      return Dataspace(id, internal::Tag());
    }
    
    static Dataspace create_scalar() 
    {
      hid_t id = H5Screate(H5S_SCALAR);
      if (id < 0)
        throw Exception("unable to create scalar dataspace");
      return Dataspace(id, internal::Tag());
    }
    
    int get_rank() const
    {
      int r = H5Sget_simple_extent_ndims(this->id);
      if (r < 0)
        throw Exception("unable to get dataspace rank");
      return r;
    }
    
    void get_dims(hsize_t *dims) const
    {
      int r = H5Sget_simple_extent_dims(this->id, dims, NULL);
      if (r < 0)
        throw Exception("unable to get dataspace dimensions");
    }

    std::vector<int> get_dims() const
    {
      std::vector<hsize_t> x(get_rank());
      int r = H5Sget_simple_extent_dims(this->id, &x[0], NULL);
      if (r < 0)
        throw Exception("unable to get dataspace dimensions");
      std::vector<int> ret(x.size());
      for (int i=0; i<r; ++i) ret[i] = x[i];
      return ret;
    }
    
    bool is_simple() const
    {
      htri_t r = H5Sis_simple(this->id);
      if (r < 0)
        throw Exception("unable to determine if dataspace is simple");
      return r > 0;
    }
    
    hssize_t get_count() const
    {
      hssize_t r = H5Sget_simple_extent_npoints(this->id);
      if (r <= 0)
        throw Exception("unable to determine number of elements in dataspace");
      return r;
    }

    void select_hyperslab(hsize_t* offset, hsize_t* stride, hsize_t* count, hsize_t *block)
    {
      herr_t r= H5Sselect_hyperslab(get_id(), H5S_SELECT_SET, offset, stride, count, block);
      if (r < 0)
        throw Exception("unable to select hyperslab");
    }

    void select_all()
    {
      herr_t r = H5Sselect_all(get_id());
      if (r < 0)
        throw Exception("unable to select all");
    }

    hssize_t get_select_npoints() const
    {
      hssize_t r = H5Sget_select_npoints(get_id()) ;
      if (r < 0)
        throw Exception("unable to get number of selected points");
      return r;
    }
    
};


inline Dataspace create_dataspace(int dim0, int dim1 = 0, int dim2 = 0, int dim3 = 0, int dim4 = 0, int dim5 = 0)
{
  enum { MAX_DIM = 6 };
  const hsize_t xa[MAX_DIM] = { (hsize_t)dim0, (hsize_t)dim1, (hsize_t)dim2, (hsize_t)dim3, (hsize_t)dim4, (hsize_t)dim5 };
  hsize_t rank = 0;
  for (; rank<MAX_DIM; ++rank) { // find rank
    if (xa[rank] <= 0) break;
  }
  if (rank <= 0)
    throw Exception("nd dataspace with rank 0 not permitted, use create_scalar()");
  return Dataspace::create_nd(xa, rank);
}



class Attribute : public Object
{
  friend class Attributes;
  friend class RWattribute;
    using Object::inc_ref;
  private:       
    Attribute(hid_t id, internal::Tag) : Object(id) {}
      
    Attribute(hid_t loc_id, const std::string &name, hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id, internal::TagCreate)
    {
      this->id = H5Acreate2(loc_id, name.c_str(), type_id, space_id, acpl_id, aapl_id);
      if (this->id < 0)
        throw Exception("error creating attribute: "+name);
    }
        
    Attribute(hid_t obj_id, const std::string &name, hid_t aapl_id, internal::TagOpen)
    {
      htri_t e = H5Aexists(obj_id, name.c_str());
      if (e == 0)
        throw NameLookupError(name);
      else if (e < 0)
        throw Exception("error checking presence of attribute: "+name);
      this->id = H5Aopen(obj_id, name.c_str(), aapl_id);
      if (this->id < 0)
        throw Exception("error opening attribute: "+name);
    }
        
  public:
    Attribute() {}

    Dataspace get_dataspace() const
    {
      hid_t id = H5Aget_space(this->id);
      if (id < 0)
        throw Exception("unable to get dataspace of Attribute");
      return Dataspace(id, internal::Tag());
    }

    Datatype get_datatype() const
    {
      hid_t type_id = H5Aget_type(this->id);
      if (type_id<0)
        throw Exception("unable to get type of Attribute");
      return Datatype(type_id);
    }

    /* assuming that the memory dataspace equals disk dataspace */
    template<class T>
    void read(Dataspace mem_space, T *values) const;

    /* assuming that the memory dataspace equals disk dataspace */
    template<class T>
    void write(Dataspace mem_space, T* values);
};


class RWattribute : public RW
{
  Attribute a;
public:
  RWattribute(hid_t id) : a(id, internal::Tag()) { a.inc_ref(); }
  Datatype get_datatype() const { return a.get_datatype(); }
  void write(const Datatype &dt, const void* buf)
  {
    herr_t err = H5Awrite(a.get_id(), dt.get_id(), buf);
    if (err < 0)
      throw Exception("error writing to attribute");
  }
  void read(const Datatype &dt, void *buf)
  {
    herr_t err = H5Aread(a.get_id(), dt.get_id(), buf);
    if (err < 0)
      throw Exception("error reading from attribute");
  }
};




template<class T>
inline void Attribute::read(Dataspace mem_space, T *values) const
{
  RWattribute rw(this->get_id());
  h5traits_of<T>::type::read(rw, mem_space, values);
}

template<class T>
inline void Attribute::write(Dataspace mem_space, T *values)
{
  RWattribute rw(this->get_id());
  h5traits_of<T>::type::write(rw, mem_space, values);
}



class Attributes
{
  private:
    Object attributed_object;
    
  public:
    Attributes() : attributed_object() {}
    Attributes(const Object &o) : attributed_object(o) {}

    Attribute open(const std::string &name)
    {
      return Attribute(attributed_object.get_id(), name, H5P_DEFAULT, internal::TagOpen());
    }

    template<class T>
    Attribute create(const std::string &name, const Dataspace &space)
    {
      Datatype disktype = my::h5::get_disktype<T>();
      Attribute a(attributed_object.get_id(),
                  name,
                  disktype.get_id(),
                  space.get_id(),
                  H5P_DEFAULT, H5P_DEFAULT,
                  internal::TagCreate());
      return a;
    }

    template<class T>
    void create(const std::string &name, const T &value)
    {
      auto sp = Dataspace::create_scalar();
      create<T>(name, sp).write(sp, &value);
    }

    template<class T>
    void create(const std::string &name, Dataspace space, const T *values)
    {
      create<T>(name, space).write(space, values);
    }

    /*
      If the attribute already exists, this function will first try
      to write the data using the given Dataspace, and then return. If this fails, the
      attribute is deleted and a new attribute is created using the provided dataspace.
    */
    template<class T>
    void set(const std::string &name, Dataspace space, const T* values)
    {
      Attribute a;
      if (exists(name))
      {
        AutoErrorReportingGuard disableErrorOutput;
        try
        {
          a = open(name);
          a.write(space, values);
          return;
        }
        catch (const Exception &)
        {
          remove(name);
        }
      }
      a = create<T>(name, space);
      a.write(space, values);
      return;
    }

    template<class T>
    void set(const std::string &name, const T &value)
    {
      set(name, Dataspace::create_scalar(), &value);
    }
    
    bool exists(const std::string &name) const
    {
      htri_t res = H5Aexists_by_name(attributed_object.get_id(), ".", name.c_str(), H5P_DEFAULT);
      if (res>0) return true;
      else if (res==0) return false;
      else throw Exception("error looking for attribute by name");
    }

    int size() const
    {
      hid_t objid = attributed_object.get_id();
      H5O_info_t info;
      herr_t err = H5Oget_info(objid, &info);
      if (err < 0)
        throw Exception("error getting the number of attributes on an object");
      return info.num_attrs;
    }

    template<class T>
    void get(const std::string &name, T &value)
    {
      Dataspace ds = Dataspace::create_scalar();
      open(name).read(ds, &value);
    }

    template<class T>
    T get(const std::string &name)
    {
      T ret;
      get(name, ret);
      return ret;
    }

    template<class T>
    void get(const std::string &name, Dataspace space, T* values)
    {
      open(name).read(space, values);
    }
    
#ifdef HDF_WRAPPER_HAS_BOOST
    template<class T>
    boost::optional<T> try_get(const std::string &name)
    {
      if (exists(name)) return boost::optional<T>(get<T>(name));
      else return boost::optional<T>();
    }
#endif

    void remove(const std::string &name)
    {
      herr_t err = H5Adelete(attributed_object.get_id(), name.c_str());
      if (err < 0)
        throw Exception("error deleting attribute");
    }
};


class Properties : protected Object
{
  public:
    using Object::get_id;
    using Object::is_valid;
    using Object::is_same;

    Properties(hid_t cls_id) : Object()
    {
      this->id = H5Pcreate(cls_id);
      if (this->id < 0)
        throw Exception("error creating property list");
    }

    Properties& deflate(int strength = 9)
    {
      H5Pset_deflate(this->id, strength);
      return *this;
    };

    Properties& chunked(int rank, const hsize_t *dims)
    {
      H5Pset_chunk(this->id, rank, dims);
      return *this;
    }

    Properties& chunked_with_estimated_size(const Dataspace &sp)
    {
      std::vector<int> dims = sp.get_dims();
      std::vector<hsize_t> cdims(dims.size());
      for (int i=0; i<dims.size(); ++i)
      {
        hsize_t val = dims[i];
        hsize_t org_val = val;
        val *= 0.1;
        if (val < 32.)
          val = 32;
        if (val > org_val)
          val = org_val;
        cdims[i] = val;
      }
      return chunked((int)cdims.size(), &cdims[0]);
    }
};


class iterator;

class Group : public Object
{
    friend class File;
	private:
		Group(hid_t loc_id, const char * name, hid_t gapl_id , internal::TagOpen)
		{
			this->id = H5Gopen2(loc_id, name, gapl_id);
			if (this->id < 0)
				throw Exception("unable to open group: "+std::string(name));
		}
		Group(hid_t loc_id, const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, internal::TagCreate)
		{
			this->id = H5Gcreate2(loc_id, name, lcpl_id, gcpl_id, gapl_id);
			if (this->id < 0)
				throw Exception("unable to create group: "+std::string(name));
		}
	public:
    
    Group() : Object() {}
    explicit Group(hid_t id) : Object(id) { Object::inc_ref(); }

    bool exists(const std::string &name) const
    {
      htri_t res = H5Lexists(this->id, name.c_str(), H5P_DEFAULT);
      if (res < 0)
        throw Exception("cannot determine existence of link");
      return res > 0;
    }

    // number of links in the group
    int size() const
    {
      H5G_info_t info;
      herr_t res = H5Gget_info(this->id, &info);
      if (res < 0)
        throw Exception("cannot get info of group");
      return info.nlinks;
    }
    
    // used to iterate over links in the group, up to size()
    std::string get_link_name(int idx) const
    {
      char buffer[4096];
      ssize_t n = H5Lget_name_by_idx(this->id, ".", H5_INDEX_NAME, H5_ITER_NATIVE, (hsize_t)idx, buffer, 4096, H5P_DEFAULT);
      if (n < 0)
        throw Exception("cannot get name of link in group");
      return std::string(buffer, n);
    }

		Group create_group(const std::string &name)
		{
			return Group(this->id, name.c_str(), H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, internal::TagCreate());
		}
		
		Group open_group(const std::string &name)
		{
			return Group(this->id, name.c_str(), H5P_DEFAULT, internal::TagOpen());
		}
		
		Group require_group(const std::string &name, bool *had_group = NULL)
		{
      if (exists(name))
      {
        if (had_group) *had_group = true;
        return open_group(name);
      }
      else
      {
        if (had_group) *had_group = false;
        return create_group(name);
      }
    }
		
		Attributes attrs() 
		{
      return Attributes(*this);
    }
    
    Dataset open_dataset(const std::string &name);

#ifdef HDF_WRAPPER_HAS_BOOST
    boost::optional<Dataset> try_open_dataset(const std::string &name);
#endif

    void remove(const std::string &name)
    {
      herr_t err = H5Ldelete(get_id(), name.c_str(), H5P_DEFAULT);
      if (err < 0)
        throw Exception("cannot remove link from group");
    }

    iterator begin();
    iterator end();
};


/*
  TODO: i probably want an iterator that returns Object references. Objects because 
  items in groups can be datasets or other groups. For this to be usefull,
  i need a mechanism to cast Object to derived classes.
*/
class iterator : public std::iterator<std::bidirectional_iterator_tag, std::string>
{
    int idx;
    Group g;
    friend class Group;
    iterator(int idx_, Group g_) : idx(idx_), g(g_) {}
  public:
    iterator()  : idx(std::numeric_limits<int>::max()) {}
    iterator& operator++() { ++idx; return *this; }
    iterator& operator--() { --idx; return *this; }
    iterator  operator++(int) { auto tmp = *this; ++idx; return tmp; }
    iterator  operator--(int) { auto tmp = *this; --idx; return tmp; }
    bool operator==(const iterator &other) const { return other.idx == idx; assert(other.g.is_same(g)); }
    bool operator!=(const iterator &other) const { return !(*this == other); }
    std::string dereference() const
    {
      return g.get_link_name(idx);
    }
    std::string operator*() const { return dereference(); }
};


inline iterator Group::begin()
{
  return iterator(0, *this);
}

inline iterator Group::end()
{
  return iterator(this->size(), *this);
}


class File : public Object
{
    File(hid_t id, internal::Tag) : Object(id) {} // takes a file handle that needs to be closed.
    friend class Object; // because Object need to construct File using the above constructor.
  public:
    explicit File(hid_t id) : Object() { this->inc_ref(); } // a logical copy of the original given by id, we inc reference count so the source can release its handle
    
    /*
      w = create or truncate existing file
      a = append to file or create new file
      r = read only; file must exist
      w- = new file; file must not already exist
      r+ = read/write; file must exist
    */
    File(const std::string &name, const std::string openmode = "w") : Object()
    {
      bool call_open = true;
      unsigned int flags; 
      if (openmode == "w")
      {
        flags = H5F_ACC_TRUNC;
        call_open = false;
      }
      else if (openmode == "a") // append at existing file or create new file
      {
        if (H5Fis_hdf5(name.c_str())>0) // file exists, so open
        {
          call_open = true;
          flags     = H5F_ACC_RDWR;
        }
        else // create file
        {
          call_open = false;
          flags     = H5F_ACC_TRUNC;
        }
      }
      else if (openmode == "w-")
      {
        flags = H5F_ACC_EXCL;
        call_open = false;
      }
      else if (openmode == "r")
        flags = H5F_ACC_RDONLY;
      else if (openmode == "r+")
        flags = H5F_ACC_RDWR;
      else
        throw Exception("bad openmode: " + openmode);
      if (call_open)
        this->id = H5Fopen(name.c_str(), flags , H5P_DEFAULT);
      else
        this->id = H5Fcreate(name.c_str(), flags , H5P_DEFAULT, H5P_DEFAULT);
      if (this->id < 0)
        throw Exception("unable to open file: " + name);
    }
    
    File() : Object() {}
    
    void open(const std::string &name, const std::string openmode = "w")
    {
      this->~File();
      new (this) File(name, openmode);
    }
    
    void close()
    {
      if (this->id == -1) return;
      herr_t err = H5Fclose(this->id);
      this->id = -1;
      if (err < 0)
        throw Exception("unable to close file");      
    }
    
    Group root()
    {
      return Group(this->id, "/", H5P_DEFAULT, internal::TagOpen());
    }

    void flush()
    {
      herr_t err = H5Fflush(this->id, H5F_SCOPE_LOCAL);
      if (err < 0)
        throw Exception("unable to flush file");
    }
};



inline File Object::get_file() const
{
  hid_t fid = H5Iget_file_id(id); // it seems to increase the reference count by its own. (The returned id acts as copy of the file reference)
  // https://www.hdfgroup.org/HDF5/doc/RM/RM_H5I.html#Identify-GetFileId
  if (fid < 0)
    throw Exception("cannot get file id from object");
  return File(fid, internal::Tag());
}


enum DsCreationFlags
{
  NONE = 0,
  COMPRESSED = 1,
  CHUNKED = 2,
#ifndef HDF_WRAPPER_DS_CREATION_DEFAULT_FLAGS
  #ifdef H5_HAVE_FILTER_DEFLATE
    DEFAULT_FLAGS = COMPRESSED
  #else
    DEFAULT_FLAGS = NONE
  #endif
#else
  DEFAULT_FLAGS = HDF_WRAPPER_DS_CREATION_DEFAULT_FLAGS
#endif
};





class Dataset : public Object
{
    friend class Group;
  private:
    Dataset(hid_t loc_id, const std::string &name, hid_t dapl_id, internal::TagOpen)
    {
      this->id = H5Dopen2(loc_id, name.c_str(), dapl_id);
      if (this->id < 0)
        throw Exception("unable to open dataset: "+name);
    }

    template<class T>
    void write(hid_t disk_space_id, hid_t mem_space_id, const T* data)
    {
      Datatype dt = get_memtype<T>();
      if (dt.get_size() == H5T_VARIABLE)
        throw Exception("not implemented");
      herr_t res = H5Dwrite(id, dt.get_id(),
                            mem_space_id, disk_space_id,
                            H5P_DEFAULT,
                            (const void*)data);
      if (res < 0)
        throw Exception("error writing to dataset"+get_name());
    }

    Dataset(hid_t id, internal::Tag) : Object(id) {} // we get an existing reference, no need to increase the ref count. it will only be lowered by one when the instance is destroyed.
    
  public:
    Dataset() : Object() {}
    explicit Dataset(hid_t id) : Object(id) { Object::inc_ref(); } // we manage the new reference
    
    static Dataset create(Group group, const std::string &name, const Datatype& dtype, const Dataspace &space, const Properties &prop)
    {
      hid_t id = H5Dcreate2(group.get_id(), name.c_str(),
                            dtype.get_id(), space.get_id(),
                            H5P_DEFAULT, prop.get_id(), H5P_DEFAULT);
      if (id < 0)
        throw Exception("error creating dataset: "+name);
      return Dataset(id, internal::Tag());
    }

    template<class T>
    void write(const T* data)
    {
      write(H5S_ALL, H5S_ALL, data);
    }

    template<class T>
    static Dataset create_simple(Group group, const std::string &name, const Dataspace &sp, const T* data, DsCreationFlags flags = DEFAULT_FLAGS, const Datatype &disktype = get_disktype<T>())
    {
      Dataset ds = Dataset::create(group, name, disktype, sp, Dataset::create_creation_properties(sp, flags));
      if (data)
        ds.write<T>(data);
      return ds;
    }

    template<class T>
    static Dataset create_scalar(Group group, const std::string &name, const T& data, const Datatype &disktype = get_disktype<T>())
    {
      Dataspace sp = Dataspace::create_scalar();
      Dataset ds = Dataset::create(group, name, disktype, sp, Dataset::create_creation_properties(sp, NONE));
      ds.write<T>(&data);
      return ds;
    }
    
    template<class T>
    void write(const Dataspace &file_space, const Dataspace &mem_space, const T* data)
    {
      write(file_space.get_id(), mem_space.get_id(), data);
    }
    
    static Properties create_creation_properties(const Dataspace &sp, DsCreationFlags flags)
    {
      Properties prop(H5P_DATASET_CREATE);
      if (flags & COMPRESSED)
        prop.deflate();
      if (flags & CHUNKED || flags & COMPRESSED)
        prop.chunked_with_estimated_size(sp);
      return prop;
    }
    
    Attributes attrs()
    {
      return Attributes(*this);
    }

    Dataspace get_dataspace() const
    {
      hid_t id = H5Dget_space(this->id);
      if (id < 0)
        throw Exception("unable to get dataspace of dataset");
      return Dataspace(id, internal::Tag());
    }

    template<class T>
    void read_simple(T *data) const
    {
      Datatype dt = get_memtype<T>();
      if (dt.get_size() == H5T_VARIABLE)
        throw Exception("not implemented");
      herr_t res = H5Dread(this->id, dt.get_id(),
                           H5S_ALL, H5S_ALL,
                           H5P_DEFAULT,
                           (void*)data);
      if (res < 0)
        throw Exception("error reading from dataset");
    }
};





inline Dataset Group::open_dataset(const std::string &name)
{
  return Dataset(this->id, name, H5P_DEFAULT, internal::TagOpen()); 
}

#ifdef HDF_WRAPPER_HAS_BOOST
inline boost::optional<Dataset> Group::try_open_dataset(const std::string &name)
{
  hid_t id;
  {
    AutoErrorReportingGuard guard;
    guard.disableReporting();
    id = H5Dopen2(this->id, name.c_str(), H5P_DEFAULT);
  }
  if (id < 0) 
  {
    if (exists(name)) // well so the dataset exists but for some reason it cannot be opened -> error
      throw Exception("unable to open existing item as dataset: "+name);
    else
      return boost::optional<Dataset>(); // no dataset under this name
  }
  else return boost::optional<Dataset>(Dataset(id, internal::Tag()));
}
#endif



template<class T>
struct h5traits
{
  static inline Datatype value(DatatypeSelect sel)
  {
    return Datatype::createPod<T>(sel);
  }
  
  static inline void write(RW &rw, const Dataspace &space, const T *values)
  {
    rw.write(value(IN_MEM), values);
  }
  
  static inline void read(RW &rw, const Dataspace &space, T *values)
  {
    rw.read(value(IN_MEM), values);
  }
};

template<>
struct h5traits<std::string>
{
  static inline Datatype value(DatatypeSelect sel)
  {
    return Datatype::createPod<const char*>(sel);
  }

  static inline void write(RW &rw, const Dataspace &space, const std::string *values)
  {
    int n = space.get_count();
    assert(n >= 1);
    std::vector<const char*> s(n);
    for (int i=0; i<n; ++i) s[i] = values[i].c_str();
    rw.write(value(IN_MEM), &s[0]);
  }

  static inline void read(RW &rw, const Dataspace &space, std::string *values)
  {
    int n = space.get_count();
    assert(n >= 1);
    Datatype disc_dt = rw.get_datatype();
    if (H5Tis_variable_str(disc_dt.get_id())==0)
    {
      Datatype dt = Datatype::createFixedLenString(IN_MEM);
      std::size_t len = disc_dt.get_size();
      len += 1; // for null termi
      dt.set_size(len);
      std::vector<char> s(n*len);
      rw.read(dt, &s[0]);
      for (std::size_t i=0; i<n; ++i)
      {
        values[i].assign(&s[i*len]);
      }
    }
    else
    {
      // see https://www.hdfgroup.org/HDF5/doc1.6/UG/11_Datatypes.html
      // variable data sets. string types (char*, char[n], std::string) are mapped by default to variable length data types
      Datatype dt = value(IN_MEM);
      assert(H5Tis_variable_str(dt.get_id()));
      int n = space.get_count();
      std::vector<char*> buffers(n);
      rw.read(dt, &buffers[0]);
      for (int i = 0; i < n; ++i)
      {
        values[i].assign(buffers[i]);
      }
      // release the stuff that hdf5 allocated
      H5Dvlen_reclaim(dt.get_id(), space.get_id(), H5P_DEFAULT, &buffers[0]);
    }
  }
};

template<size_t n>
struct h5traits<char[n]>
{
  typedef char CharArray[n];
  static inline Datatype value(DatatypeSelect sel)
  {
    return Datatype::createPod<const char*>(sel);
  }

  static inline void write(RW &rw, const Dataspace &space, const CharArray *values)
  {
    std::vector<const char*> s(space.get_count());
    for (int i=0; i<s.size(); ++i) s[i] = values[i];
    h5traits<const char*>::write(rw, space, &s[0]);
  }
};

//// const char[n] added for visual c++2013
//template<size_t n>
//struct as_h5_datatype<const char [n]>
//{
//  typedef char CharArray[n];
//  static inline Datatype value(DatatypeSelect sel) { return as_h5_datatype<char[n]>::value(sel); }
//  static inline void write(RW &rw, const Dataspace &space, const CharArray *values)  { return as_h5_datatype<char[n]>::write(rw, space, values); }
//};

template<class T>
struct h5traits_of
{
  typedef typename std::remove_cv<T>::type stripped_type;
  typedef h5traits<stripped_type> type;
};

template<class T>
inline Datatype get_disktype()
{
  return h5traits_of<T>::type::value(ON_DISK);
}

template<class T>
inline Datatype get_memtype()
{
  return h5traits_of<T>::type::value(IN_MEM);
}

} // namespace h5

} // namespace my

#endif
