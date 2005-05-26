import PMQ;

private mapping props = ([]);

//!
mixed get(string key)
{
  return props[key];
}

//!
void set(string key, mixed val)
{
  props[key] = val;
}

//! read the contents of a properties file into the object.
void load(string props)
{
  foreach(props/"\n";; string line)
  {
    array e = line/"=";
    if(sizeof(e) > 2)
      error("invalid property line: " + line + "\n");
    set(e[0], e[1]);
  }
}

//!
array _indices()
{
  return indices(props);
}

//!
array _values()
{
  return values(props);
}
