import PMQ;
inherit PMQWriter;

string topic;

string get_topic()
{
  return this->topic;
}

void set_topic(string topic)
{
  this->topic = topic;
}
