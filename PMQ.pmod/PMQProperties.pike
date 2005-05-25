import PMQ;

mapping props = ([]);

mixed get(string key)
{
  return props[key];
}

void set(string key, mixed val)
{
  props[key] = val;
}

