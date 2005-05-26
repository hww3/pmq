int i = 0;
import PMQ;
string name;
inherit .PMQQueue;
import PMQConstants;

Thread.Mutex lock = Thread.Mutex();

ADT.Queue q;

int ack = 0;
int processing = 0;
multiset listeners = (<>);
multiset writers = (<>);
multiset waiters = (<>);

void create(string name)
{
  this->name = name;
  q = ADT.Queue();
  call_out(queuesize, 5);
}

void queuesize()
{
  werror("%d messages in %s\n", sizeof((array)q), name);
  call_out(queuesize,5);
}
// a return value of 0 indicates we're already waiting for a message.
int get_message(PMQ.PMQSSession s)
{
  if(!listeners[s])
  {
     DEBUG(2, "Session %O not already subscribed.\n");
     return 0;
  }
  if(waiters[s]) return -1;
  else
    waiters[s] = 1;

  call_out(process_queue, 0);
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
  Thread.MutexKey key = lock->lock();
  if(sizeof(listeners) && !q->is_empty())
  {
      Message.PMQMessage m = q->peek();

      // there should only ever be one listener.
      foreach(indices(waiters);; PMQSSession listener)
      {
        waiters[listener] = 0; // the SSession will add it back when 
                                 // it's time for another.
        if(listener->send_message(m,  ack))
        {
          q->read();
        }
      }
  }

  key = 0;
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
    //  call_out(process_queue, 0);
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
      waiters -= (< listener >);
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
