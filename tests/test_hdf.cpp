
#include <boost/foreach.hpp>
#include <boost/format.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/assign/std/vector.hpp>
#include <iostream>
#include <vector>

#include "hdf_wrapper.h"
#include "hdf_wrapper_stl.h"


using boost::str;
using boost::format;
using namespace std;
namespace h5 = h5cpp;

void WriteFile()
{
  cout << "=== Writing ===" << endl;
  cout << "-- type caching and ref. counting sanity.--" << endl;
  {
    h5::Datatype dt_int1 = h5::get_disktype<int>();
    h5::Datatype dt_int2 = h5::get_disktype<int>(); // refer to the same type object
    cout << "dt_int1 id = " << dt_int1.get_id() << " vs. dt_int2 id = " << dt_int2.get_id() << endl;
    assert(dt_int1.get_id() == dt_int2.get_id());
  }
  {
    h5::Datatype dt = h5::get_disktype<int>();
    cout << "dt ref count = " << dt.get_ref() << endl;
    assert (dt.get_ref() == 2);  // one for cached instance, and another reference accounting for the dt variable here.
  }
  
  cout << "-- group management and attribute R/W tests --" << endl;
  
  /* like in h5py:
    w = create or truncate existing file
    a = append to file or create new file
    r = read only; file must exist
    w- = new file; file must not already exist
    r+ = read/write; file must exist
  */
  h5::File file("test.h5", "w");
  h5::Group root = file.root();  // because File does not have group functionality here. This is reserved for future work.
  
  // Attributes class inspired by h5py
  h5::Attributes a = root.attrs();
  a.create<char>("a_char",'q'); // create scalar attribute
  a.set<double>("set double test", 2.); // replace attribute with new data. Can be of different type, because old attribute will just be deleted if neccessary.
  a.create<float>("to_be_deleted", 9000.0);
  a.create<int>("attrib_overwrite_test1", 1);
  a.create<int>("attrib_overwrite_test2", 2);
  a.create<string>("a_string_attr","teststring");
  a.create("c_str_attr", "it's a c string");
  try
  {
      a.create<float>("to_be_deleted", 9002.0); 
  }
  catch (h5::Exception &e)
  {
    cout << "Attempt to create two attributes under the same name correctly failed with error message:" << endl;
    cout << e.what() << endl;
  }
  a.remove("to_be_deleted");
  a.create("to_be_deleted", "really gone?");
  a.set<string>("attrib_overwrite_test1", "erased and recreated!");
  a.set<int>("attrib_overwrite_test2", 3); // should just write into the existing space
  
  // try to make a group and set some attributes
  h5::Group g = root.create_group("testing_the_group");
  assert(g.is_valid());
  g.attrs().create<double>("a1", 5.);
  g.attrs().create<int>("second_attribute", 1);

  bool group_already_there;
  h5::Group g2 = root.require_group("testing_the_group", &group_already_there);
  assert(group_already_there && g2.is_valid());
  
  {
    h5::Group g3 = root.require_group("second_group");
    assert(g3.is_valid());
  }

  h5::Group g3 = root.open_group("second_group").create_group("third_group");

  // dataspaces
  h5::Dataspace sp_scalar = h5::Dataspace::create_scalar();
  h5::Dataspace sp_all    = h5::Dataspace::create_all();
  h5::Dataspace sp1       = h5::create_dataspace(10,20,30); // three dimensional dataset with dimensions 10 x 20 x 30
  hsize_t dims[3] = { 10, 20, 30};
  h5::Dataspace sp2       = h5::Dataspace::create_nd(dims, 3);
  vector<int> dims_vec; 
  dims_vec.push_back(10);
  dims_vec.push_back(20);
  dims_vec.push_back(30);
  // following two create function are found in hdf_wrapper_stl.h
  h5::Dataspace sp5       = h5::create_dataspace_from_range(dims_vec); // calls dims_vec.begin(), dims_vec.end() 
  h5::Dataspace sp6       = h5::create_dataspace_stl((hsize_t*)dims, dims+3); // good old pointer arithmetic
  // TODO: check if these dataspaces are okay
  
  // array attributes
  int static_ints[6] = { 1, 2, 3, 4, 5, 6 };
  g3.attrs().create("ints", h5::create_dataspace(2,3), static_ints);

  // variable length string attributes
  const char* strings1[] = {
    "string1",
    "string2 long",
    "string3 very long"
  };
  g3.attrs().create("strings1", h5::create_dataspace(3), strings1); 
  
  // using the wrapper for stl constainers.
  vector<string> strings2;
  strings2.push_back("test");
  strings2.push_back("string");
  strings2.push_back("array attrib");
  h5::set_array_attribute_range(g3.attrs(), "strings2", strings2); // should use the overloaded function for std::vector
  
  cout << "-- datasets --" << endl;
  
  vector<float> data(10);
  for(int i=0; i<data.size(); ++i)
    data[i] = i*i;
  
  vector<int> sizes(1, data.size());  // one-dimensional
  h5::Dataset ds = h5::Dataset::create_simple(g, "testds", h5::create_dataspace_from_range(sizes), &data[0]);
  ds.attrs().create("someAttr", 7);
  
  // make nice data in memory
  vector<double> bigdata(dims[0]*dims[1]*dims[2]);
  int i=0;
  for (int z=0; z<dims[2]; ++z)  for (int y=0; y<dims[1]; ++y) for (int x=0; x<dims[0]; ++x)
  {
    bigdata[i++] = cos(x * M_PI/10.) * cos(y * M_PI/10.) * cos(z * M_PI/10.);
  }
  
  // most general data writing
  h5::Dataset ds2 = h5::Dataset::create(g, "bigdata", 
					h5::get_disktype<double>(), 
					sp6,
					h5::Dataset::create_creation_properties(sp6, h5::COMPRESSED));
  ds2.write(sp_all, sp_all, &bigdata[0]);  // TODO: fails because sp_all!

  cout << "-- hyperslab selection --" << endl;
  { // selection test
    h5::Dataspace sp = h5::create_dataspace(5, 2);
    h5::Dataset ds = h5::Dataset::create_simple<int>(root, "select_test_ds", sp, nullptr);
    hsize_t offset[2] = { 0, 0 };
    hsize_t stride[2] = { 1, 1 };
    hsize_t block[2] = { 1, 1 };
    hsize_t count[2] = { 4, 1 };
    sp.select_hyperslab(offset, stride, count, block); // writes to [0 .. 4] x [0 .. 0]
    
    int memdata[] = { 1, 2, 3, 4 };

    h5::Dataspace memsp = h5::create_dataspace(4);
    ds.write(sp, memsp, &memdata[0]);

    offset[0] = 1;
    offset[1] = 1;
    sp.select_hyperslab(offset, stride, count, block);
    ds.write(sp, memsp, &memdata[0]);  // writes to [1 .. 5] x [1 .. 1]
  }
}

#if 0
void ReadFile()
{
  cout << "=== Reading ===" << endl;
  
  h5::File file("test.h5", "r");
  h5::Group root = file.root();
  h5::Dataset ds = file.root().open_group("testing_the_group").open_dataset("testds");
  h5::Dataspace sp = ds.get_dataspace();
  
  vector<int> sizes = sp.get_dims();
  cout << "dataset size = ";
  BOOST_FOREACH(int q, sizes)
  {
    cout << q << " ";
  }
  cout << endl;
  
  vector<float> data(sp.get_count());
  ds.read<float>(&data[0]);
  cout << "dataset = ";
  BOOST_FOREACH(float q, data)
    cout << q << " ";
  cout << endl;
  
  string s = root.attrs().get<string>("c_str_attr");
  cout << "string attribute: " << s << endl;
  
  h5::Attributes a = ds.attrs();
  int static_ints[6];
  a.get("ints", static_ints);
  cout << "static ints";
  for (int i=0; i<6; ++i)
    cout << " " << static_ints[i];
  cout << endl;
  
  vector<string> string_vec;
  h5::Attribute at = a.open("strings");
  int size = at.get_dataspace().get_count();
  cout << "string_vec (" << size << ") ";
  cout.flush();
  string_vec.resize(size);
  at.read(&string_vec[0]);
  for (int i=0; i<string_vec.size(); ++i)
    cout << " " << string_vec[i];
  cout << endl;

  assert(root.is_valid());
  cout << root.get_name() << " has " << root.size() << " children." << endl;
  cout << "children of " << root.get_name() << endl;
  for (int i = 0; i < root.size(); ++i)
  {
    cout << "\t" << root.get_link_name(i) << endl;
  }
  cout << "and again ..." << endl;

  const auto end = root.end(); // takes a little bit of time
  for (auto it = root.begin(); it != end; ++it)
  {
    cout << "\t" << *it << endl;
  }

#if 0
  {
    cout << "reading testfield" << endl;
    Array3d<float> arr;
    LatticeDataQuad3d ld;
    ReadHdfLd(root.open_group("field_ld"), ld);
    ReadArray3D<float>(root.open_dataset("test_field"), arr);
    PrintArray<>(arr);
  }
#endif
}
#endif


int main(int argc, char **argv)
{
  h5::disableAutoErrorReporting();  // don't print to stderr
  WriteFile();
  //ReadFile();
  cin.get();
  return 0;
}
