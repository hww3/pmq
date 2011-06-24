import PMQ;
inherit PMQReader;

//constant requires_get = 0;

string topic;

void set_topic(string topic)
{
  this->topic = topic;
}

//!
string get_topic()
{ 
  return this->topic;
}
