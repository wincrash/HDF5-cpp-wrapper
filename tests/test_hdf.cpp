
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

using namespace my;


struct Failtype
{
  int a;
  float b;
};


void WriteFile()
{
  h5::File file("test.h5", "w");
  h5::Group root = file.root();
  h5::Group g = root.create_group("testing_the_group");
  g.attrs().set<double>("a1", 5.);
  g.attrs().set<int>("second_attribute", 1);
      
  h5::Attributes a = root.attrs();
  a.set<char>("achar",'q');
  a.set<float>("afloat",9000.0);
  a.set<string>("astring_attr","teststring");
  a.set("c_str_attr","it's a c string");
  
  //a = g.attrs();
  //a.set<Failtype>("failtest", Failtype());
  
  vector<float> data(10);
  for(int i=0; i<data.size(); ++i)
    data[i] = i*i;
  
  vector<int> sizes(1, data.size());
  h5::Dataset ds = h5::Dataset::create_simple(g, "testds", h5::create_dataspace_from_range(sizes), &data[0]);

  int static_ints[6] = { 1, 2, 3, 4, 5, 6 };
  ds.attrs().set_array("ints", h5::create_dataspace(2,3), static_ints);

  vector<string> ss;
  ss.push_back("test");
  ss.push_back("string");
  ss.push_back("array attrib");
  ds.attrs().set_array("strings", h5::create_dataspace(ss.size()), &ss[0]);

  const char* morestrings[] = {
    "string1",
    "string2",
    "string3"
  };
  ds.attrs().set_array("more_strings", h5::create_dataspace(3), morestrings);

  {
  h5::Group g1 = root.create_group("g1");
  }
  h5::Group g2 = root.open_group("g1").create_group("g2");

#if 0
  { // scalar field test
    Array3d<float> f;
    BBox3 bb(Int3(0), Int3(1,2,3)); 
    f.initFromBox(bb);
    LatticeDataQuad3d ld(bb, 1.);
    FOR_BBOX3(p, bb)
    {
      f(p) = p[0]*100 + p[1] * 10 + p[2];
    }
    h5::Group ld_grp = root.create_group("field_ld");
    WriteHdfLd(ld_grp, ld);
    //WriteScalarField(root, "test_field", f, ld, ld_grp);
    WriteArray3D(root, "test_field", f);
  }

  { // selection test
    h5::Dataspace sp = h5::create_dataspace(5, 2);
    h5::Dataset ds = h5::Dataset::create_simple<int>(root, "select1", sp, NULL);
    hsize_t offset[2] = { 0, 0 };
    hsize_t stride[2] = { 1, 1 };
    hsize_t block[2] = { 1, 1 };
    hsize_t count[2] = { 4, 1 };
    sp.select_hyperslab(offset, stride, count, block);
    
    vector<int> memdata; boost::assign::push_back(memdata)(1)(2)(3)(4);

    h5::Dataspace memsp = h5::create_dataspace(memdata.size());
    ds.write(sp, memsp, &memdata[0]);

    offset[0] = 1;
    offset[1] = 1;
    sp.select_hyperslab(offset, stride, count, block);
    ds.write(sp, memsp, &memdata[0]);
  }
#endif
}

void ReadFile()
{
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
  ds.read_simple<float>(&data[0]);
  cout << "dataset = ";
  BOOST_FOREACH(float q, data)
    cout << q << " ";
  cout << endl;
  
  string s = root.attrs().get<string>("c_str_attr");
  cout << "string attribute: " << s << endl;
  
  h5::Attributes a = ds.attrs();
  int static_ints[6];
  a["ints"].get_array(6, static_ints);
  cout << "static ints";
  for (int i=0; i<6; ++i)
    cout << " " << static_ints[i];
  cout << endl;
  
  vector<string> string_vec;
  h5::Attribute at = a["strings"];
  int size = at.get_dataspace().get_count();
  cout << "string_vec (" << size << ") ";
  cout.flush();
  string_vec.resize(size);
  at.get_array(string_vec.size(), &string_vec[0]);
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


int main(int argc, char **argv)
{
  WriteFile();
  ReadFile();
  cin.get();
  return 0;
}
