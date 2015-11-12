#include "hdf_wrapper.h"

struct Foo
{
  int x;
  double y;
};

int main(int argc, char **argv)
{
  h5cpp::Datatype dt1 = h5cpp::get_memtype<int>(); // ok
  
  h5cpp::Datatype dt2 = h5cpp::get_memtype<Foo>(); // err
  return 0;
}
