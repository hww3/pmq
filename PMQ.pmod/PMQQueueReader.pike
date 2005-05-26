import PMQ;
import PMQConstants;
inherit PMQReader;

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

