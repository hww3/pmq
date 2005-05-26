import PMQ;
mapping queues = ([]);
mapping topics = ([]);

void create()
{

}

object get_queue_by_name(string name)
{
  return queues[name];
}

object get_topic_by_name(string name)
{
  return topics[name];
}

object new_queue(string name, string type)
{
  if(queues[name]) error("Queue " + name + " already exists.\n");
  object q = master()->resolv("PMQ.Queue." + type)(name);
  q->start();
  queues[name] = q;
  return q;
}

object new_topic(string name, string type)
{
  if(topics[name]) error("Topic " + name + " already exists.\n");
  object q = master()->resolv("PMQ.Queue." + type)(name);
  q->start();
  topics[name] = q;
  return q;
}

void remove_queue(object q)
{

  mixed res;
  do
  {
    res = search(queues, q);
    if(res == 0 && zero_type(res) == 1) break;
    else 
    {
      if(queues[res]) destruct(queues[res]);
      m_delete(queues, res);
    }
  }
  while(1);
}

void remove_topic(object q)
{

  mixed res;
  do
  {
    res = search(topics, q);
    if(res == 0 && zero_type(res) == 1) break;
    else 
    {
      if(topics[res]) destruct(topics[res]);
      m_delete(topics, res);
    }
  }
  while(1);
}

