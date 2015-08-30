#ifndef _HDF_WRAPPER_H
#define _HDF_WRAPPER_H

#include <iostream>
#include <exception>
#include <assert.h>
#include <vector>
#include <sstream>
#include <malloc.h>

#include "hdf5.h"
#include <string.h>
#include <cstdio>

#include <boost/optional.hpp>

class T;
namespace my
{

namespace h5
{

class Object;
class Dataspace;
class Dataset;
class Attributes;
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
  H5Eset_auto2(H5E_DEFAULT, NULL, NULL);
};

class Exception : public std::exception
{
		std::string msg;
	public:
		Exception(const std::string &msg_) : msg(msg_)
    {
      char *mem;
      size_t size;
      FILE* stream = open_memstream(&mem, &size);
      H5Eprint2(H5E_DEFAULT, stream);
      fclose(stream);
      msg += "\n";
      msg += std::string(mem);
      free(mem);
    }
		Exception() : msg("unspecified error") { assert(false); }
		~Exception() throw() {}
		const char* what() const throw() { return msg.c_str(); }
};

class NameLookupError : public Exception
{
  public:
    NameLookupError(const std::string &name) : Exception("cannot find '"+name+"'") {}
};


class Object
{
  public:
    Object(const Object &o) : id(o.id) 
    {
      inc_ref();
    }

    ~Object() 
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

    Object(hid_t id) : id(id)
    {
      htri_t ok = H5Iis_valid(id);
      if (!ok)
        throw Exception("initialization of Object with invalid handle");
    }
    
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

		hid_t id;
};

enum DatatypeSelect {
  ON_DISK,
  IN_MEM
};


class Datatype : protected Object
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
    using Object::get_id;
    using Object::get_name;
    using Object::is_valid;
    using Object::get_file_name;
    using Object::get_file;

    Datatype() : Object() {}
    
    template<class T>
    static Datatype createPod(DatatypeSelect)
    {
      //static_assert(false, "specialize template<class T> Datatype as_h5_datatype(DatatypeSelect) for this type");
      assert(false);
      throw Exception("specialize template<class T> Datatype as_h5_datatype(DatatypeSelect) for this type");
      return Datatype(-1, ConsFromPreset()); 
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

#define SPECIALIZE_TYPE(t, tid, dtid) \
  template<> inline Datatype Datatype::createPod<t>(DatatypeSelect sel) { return Datatype(sel == ON_DISK ? dtid : tid, ConsFromPreset()); }\

SPECIALIZE_TYPE(int, H5T_NATIVE_INT, H5T_STD_I32LE)
SPECIALIZE_TYPE(unsigned int, H5T_NATIVE_UINT, H5T_STD_U32LE)
SPECIALIZE_TYPE(unsigned long long, H5T_NATIVE_ULLONG, H5T_STD_U64LE)
SPECIALIZE_TYPE(long long, H5T_NATIVE_LLONG, H5T_STD_I64LE)
SPECIALIZE_TYPE(char, H5T_NATIVE_CHAR, H5T_STD_I8LE)
SPECIALIZE_TYPE(unsigned char, H5T_NATIVE_UCHAR, H5T_STD_U8LE)
SPECIALIZE_TYPE(float, H5T_NATIVE_FLOAT, H5T_IEEE_F32LE)
SPECIALIZE_TYPE(double, H5T_NATIVE_DOUBLE, H5T_IEEE_F64LE)
SPECIALIZE_TYPE(bool, H5T_NATIVE_CHAR, H5T_STD_U8LE)
SPECIALIZE_TYPE(unsigned long, H5T_NATIVE_ULONG, H5T_STD_U64LE)
SPECIALIZE_TYPE(long, H5T_NATIVE_LONG, H5T_STD_I64LE)

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



class RW
{
  public:
    virtual void write(const Datatype &dt, const void* buf) = 0;
    virtual void read(const Datatype &dt, void* buf) = 0;
    virtual Datatype get_datatype() const = 0;
    virtual ~RW() {}
};



namespace user
{

template<class T>
struct as_h5_datatype;

}





class Dataspace : protected Object
{
    friend class Dataset;
    friend class Attributes;
    friend class Attribute;
  private:
    Dataspace(hid_t id, internal::Tag) : Object(id) {}
  
  public:
    using Object::get_id;
    using Object::get_name;
    using Object::is_valid;

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
    
    void get_dims(hsize_t *dims)
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




class Attribute : protected Object
{
  friend class Attributes;
  friend class RWattribute;
    using Object::inc_ref;
  private:
    using Object::get_id;
    using Object::is_valid;
    using Object::get_file_name;
        
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

    template<class T>
    void get_array(int buffer_size, T *values) const;

    template<class T>
        void get(T &value) const
    {
      get_array<T>(1, &value);
    }

    template<class T>
        T get() const
    {
      T r;
      get<T>(r);
      return r;
    }
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
inline void Attribute::get_array(int buffer_size, T *values) const
{
  Dataspace ds = get_dataspace();
  if (ds.get_count() != buffer_size)
    throw Exception("buffer_size argument does not match dataspace size");
  RWattribute rw(get_id());
  user::as_h5_datatype<T>::read(rw, get_dataspace(), values);
}


class Attributes
{
  private:
    Object attributed_object;
    
  public:
    Attributes() : attributed_object() {}
    Attributes(const Object &o) : attributed_object(o) {}


#if 1
    Attribute open(const std::string &name)
    {
      return Attribute(attributed_object.get_id(), name, H5P_DEFAULT, internal::TagOpen());
    }

    Attribute operator[](const std::string &name)
    {
      return open(name);
    }
    
    bool exists(const std::string &name) const
    {
      htri_t res = H5Aexists_by_name(attributed_object.get_id(), ".", name.c_str(), H5P_DEFAULT);
      if (res>0) return true;
      else if (res==0) return false;
      else throw Exception("error looking for attribute by name");
    }
    
    // setters
    template<class T>
    void set_array(const std::string &name, const Dataspace &space, const T* values)
    {
      Datatype disktype = user::as_h5_datatype<T>::value(ON_DISK);
      Attribute a(attributed_object.get_id(),
                  name,
                  disktype.get_id(),
                  space.get_id(),
                  H5P_DEFAULT, H5P_DEFAULT,
                  internal::TagCreate());
      RWattribute rw(a.get_id());
      user::as_h5_datatype<T>::write(rw, user::as_h5_datatype<T>::value(IN_MEM), space, values);
    }

    template<class T>
    void set(const std::string &name, const T &value)
    {
      set_array(name, Dataspace::create_scalar(), &value);
    }

    // getters
    template<class T>
    void get(const std::string &name, T &value)
    {
      open(name).get(value);
    }

    template<class T>
    T get(const std::string &name)
    {
      return open(name).get<T>();
    }
    
    template<class T>
    boost::optional<T> try_get(const std::string &name)
    {
      if (exists(name)) return boost::optional<T>(get<T>(name));
      else return boost::optional<T>();
    }

#else
    template<class T>
    void set(const std::string &name, const T &value)
    {
      Datatype disktype = user::as_h5_datatype<T>::value(ON_DISK);
      Dataspace diskspace = Dataspace::create_scalar();
      Attribute a(attributed_object.get_id(),
                  name,
                  disktype.get_id(),
                  diskspace.get_id(),
                  H5P_DEFAULT, H5P_DEFAULT, 
                  internal::TagCreate());
      RWattribute rw(a.get_id());
      user::as_h5_datatype<T>::write(rw, user::as_h5_datatype<T>::value(IN_MEM), value);
    }
    
    template<class T>
    T get(const std::string &name)
    {
      Attribute a(attributed_object.get_id(),
                  name,
                  H5P_DEFAULT,
                  internal::TagOpen());
      RWattribute rw(a.get_id());;
      return user::as_h5_datatype<T>::read(rw, user::as_h5_datatype<T>::value(IN_MEM));
    }

    template<class T>
        void get(const std::string &name, T &value)
    {
      value = get<T>(name);
    }
#endif
};


class Properties : protected Object
{
  public:
    using Object::get_id;
    using Object::is_valid;

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
      return chunked(cdims.size(), &cdims[0]);
    }
};




class Group : protected Object
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
    using Object::get_id;
    using Object::get_name;
    using Object::is_valid;
    using Object::get_file_name;
    using Object::get_file;
    
    Group() : Object() {}
    explicit Group(hid_t id) : Object(id) { Object::inc_ref(); }
//     ~Group() {
//       H5Gclose(this->id);
//       this->id = -1;
//     }

    bool exists(const std::string &name) const
    {
      htri_t res = H5Lexists(this->id, name.c_str(), H5P_DEFAULT);
      if (res < 0)
        throw Exception("cannot determine existence of link");
      return res > 0;
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

    boost::optional<Dataset> try_open_dataset(const std::string &name);
};


class File : protected Object
{
    explicit File(hid_t id, internal::Tag) : Object(id) {}
    friend class Object;
  public:
    using Object::get_id;
    using Object::get_name;
    using Object::is_valid;
    using Object::get_file_name;
    using Object::get_file;

    explicit File(hid_t id) : Object() { this->inc_ref(); } // a logical copy of the original given by id
    
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
  if (fid < 0)
    throw Exception("cannot get file id from object");
  return File(fid, internal::Tag());
}


enum DsCreationFlags
{
  NONE = 0,
  COMPRESSED = 1,
  CHUNKED = 2,
#ifdef H5_HAVE_FILTER_DEFLATE
  MW_HDF5_DEFAULT_FILTER = COMPRESSED
#else
  MW_HDF5_DEFAULT_FILTER = NONE
#endif
};


//class DatasetSelection
//{
//  Dataspace space;
//};


class Dataset : protected Object
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
      Datatype dt = user::as_h5_datatype<T>::value(IN_MEM);
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
    using Object::get_id;
    using Object::get_name;
    using Object::is_valid;
    using Object::get_file_name;
    using Object::get_file;

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
    static Dataset create_simple(Group group, const std::string &name, const Dataspace &sp, const T* data, DsCreationFlags flags = MW_HDF5_DEFAULT_FILTER, const Datatype &disktype = user::as_h5_datatype<T>::value(ON_DISK))
    {
      Dataset ds = Dataset::create(group, name, disktype, sp, Dataset::create_creation_properties(sp, flags));
      if (data)
        ds.write<T>(data);
      return ds;
    }

    template<class T>
    static Dataset create_scalar(Group group, const std::string &name, const T& data, const Datatype &disktype = user::as_h5_datatype<T>::value(ON_DISK))
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
      Datatype dt = user::as_h5_datatype<T>::value(IN_MEM);
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


inline boost::optional<Dataset> Group::try_open_dataset(const std::string &name)
{
  hid_t id = H5Dopen2(this->id, name.c_str(), H5P_DEFAULT);
  if (id < 0) 
  {
    if (exists(name))
      throw Exception("unable to open existing item as dataset: "+name);
    else
      return boost::optional<Dataset>();
  }
  else return boost::optional<Dataset>(Dataset(id, internal::Tag()));
}




namespace user
{
#if 1
  template<class T>
  struct as_h5_datatype
  {
    static inline Datatype value(DatatypeSelect sel)
    {
      return Datatype::createPod<T>(sel);
    }
  
    static inline void write(RW &rw, const Datatype &dt, const Dataspace &space, const T *values)
    {
      rw.write(dt, values);
    }
  
    static inline void read(RW &rw, const Dataspace &space, T *values)
    {
      rw.read(value(IN_MEM), values);
    }
  };

  template<>
  struct as_h5_datatype<std::string>
  {
    static inline Datatype value(DatatypeSelect sel)
    {
      return Datatype::createPod<const char*>(sel);
    }

    static inline void write(RW &rw, const Datatype &dt, const Dataspace &space, const std::string *values)
    {
      int n = space.get_count();
      assert(n >= 1);
      std::vector<const char*> s(n);
      for (int i=0; i<n; ++i) s[i] = values[i].c_str();
      rw.write(dt, &s[0]);
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
        Datatype dt = value(IN_MEM);
        int n = space.get_count();
        assert(n >= 1);
        std::vector<char*> s(n);
        rw.read(dt, &s[0]);
        for (int i=0; i<n; ++i)
        {
          values[i].assign(s[i]);
          free(s[i]);
        }
      }
    }
  };

  template<size_t n>
  struct as_h5_datatype<char[n]>
  {
    typedef char const CharArray[n];
    static inline Datatype value(DatatypeSelect sel)
    {
      return Datatype::createPod<const char*>(sel);
    }

    static inline void write(RW &rw, const Datatype &dt, const Dataspace &space, const CharArray *values)
    {
      std::vector<const char*> s(space.get_count());
      for (int i=0; i<s.size(); ++i) s[i] = values[i];
      as_h5_datatype<const char*>::write(rw, dt, space, &s[0]);
    }
  };

#else
template<class T>
struct as_h5_datatype
{
  static inline Datatype value(DatatypeSelect sel)
  {
    return Datatype::as_h5<T>(sel);
  }
  
  static inline void write(RW &rw, const Datatype &dt, const T &value)
  {
    rw.write(dt, &value);
  }
  
  static inline T read(RW &rw, const Datatype &dt)
  {
    T t;
    rw.read(dt, &t);
    return t;
  }
};


template<>
struct as_h5_datatype<std::string>
{
  static inline Datatype value(DatatypeSelect sel)
  {
    return Datatype::as_h5<const char*>(sel);
  }

  static inline void write(RW &rw,  const Datatype &dt, const std::string &value)
  {
    const char* s = value.c_str();
    rw.write(dt, &s);
  }

  static inline std::string read(RW &rw, const Datatype &dt)
  {
    char* s = NULL;
    rw.read(dt, &s);
    std::string t(s);
    free(s);
    return t;
  }
};



template<size_t n>
struct as_h5_datatype<char[n]>
{
  static inline Datatype value(DatatypeSelect sel)
  {
    return Datatype::as_h5<const char*>(sel);
  }
  
  static inline void write(RW &rw, const Datatype &dt, const char (&value) [n])
  {
    as_h5_datatype<const char*>::write(rw, dt, value);
  }
};

#endif
} // namespace user

} // namespace h5

} // namespace my

#endif
