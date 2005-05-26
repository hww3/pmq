import PMQ;
string name;
inherit .PMQQueue;
import PMQConstants;

ADT.Queue q;

int ack = 0;
int processing = 0;
multiset listeners = (<>);
multiset writers = (<>);

void create(string name)
{
  this->name = name;
// write("Queue " + name + " created.\n");
  q = ADT.Queue();
//  call_out(trigger_process_queue, 5);
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
    q->write(message);
  }
  else return 0;
  call_out(process_queue, 0);
  return 1;
}

void process_queue()
{
  if(processing) return;
  if(sizeof(listeners) && !q->is_empty())
  {
    processing = 1;
    do
    {
      Message.PMQMessage m = q->peek();

      foreach(indices(listeners);; PMQSSession listener)
      {
        if(listener->send_message(m,  ack)) q->read();
        else
        {
           DEBUG(1, "%O: Delivery failed; aborting processing.\n", this);
           processing = 0;
           return;
        }
      }
    } while(!q->is_empty());
  }

  processing = 0;
}

int subscribe(PMQSSession listener)
{
 write("Queue " + name + " subscribe.\n");
  if(listener->get_mode() == MODE_LISTEN)
  {
    if(sizeof(listeners) == 0)
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
  write("Queue " + name + " unsubscribe.\n");

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
  return "PMQQueue(" + name + ")";
}
