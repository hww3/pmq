import PMQ;
inherit PMQReader;

string topic;

void set_topic(string topic)
{
  this->topic = topic;
}

string get_topic()
{ 
  return this->topic;
}
