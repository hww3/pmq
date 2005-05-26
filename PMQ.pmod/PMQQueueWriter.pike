import PMQ;
inherit PMQWriter;

string queue;

//!
string get_queue()
{
  return this->queue;
}

void set_queue(string queue)
{
  this->queue = queue;
}
