import PMQ;
inherit .PMQQueue;
string name;

import PMQConstants;

int ack = 0;
int processing = 0;
multiset listeners = (<>);
multiset writers = (<>);
void create(string name)
{
  this->name = name;
// write("Topic " + name + " created.\n");
  q = ADT.Queue();
//  call_out(trigger_process_queue, 5);
}

void process_queue()
{
}

void trigger_process_queue()
{
  if(!processing)
    process_queue();

  call_out(trigger_process_queue, 5);

}

void start()
{
}

void stop()
{
}

int post_message(Message.PMQMessage message, PMQSSession session)
{
  if(writers[session])
  {
    process(message);
  }
  else return 0;
  call_out(process_queue, 0);
  return 1;
}

void process(Message.PMQMessage message)
{
    processing = 1;
  if(sizeof(listeners))
  {
      foreach(indices(listeners);; PMQSSession listener)
      {
        listener->send_message(message, ack);
      }
  }

  processing = 0;
}

int subscribe(PMQSSession listener)
{
 write("Queue " + name + " subscribe.\n");
  if(listener->get_mode() == MODE_LISTEN)
  {
//    if(sizeof(listeners) == 0)
    if(1)
    {
      listeners += (< listener >);
      listener->set_queue(this);
      call_out(process_queue, 0);
      return CODE_SUCCESS;
    }
  }
  else if(listener->get_mode() == MODE_WRITE)
  {
    writers += (< listener >);
    listener->set_queue(this);
    return CODE_SUCCESS;
  }

  else return CODE_NOSLOTS;
}

int unsubscribe(PMQSSession listener)
{
  write("Topic " + name + " unsubscribe.\n");

  if(listener->get_mode() == MODE_LISTEN)
  {
    if(sizeof(listeners) != 0 && listeners[listener])
    {
      listeners -= (< listener >);
      return CODE_SUCCESS;
    }
  }
  else if(listener->get_mode() == MODE_WRITE)
  {
    if(sizeof(writers) != 0 && writers[listener])
    {
      writers -= (< listener >);
      return CODE_SUCCESS;
    }
  }
  else return CODE_FAILURE;
}

string _sprintf(mixed args)
{
  return "PMQTopic(" + name + ")";
}
