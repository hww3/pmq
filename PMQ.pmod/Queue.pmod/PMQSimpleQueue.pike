int i = 0;
import PMQ;
string name;
inherit .PMQQueue;
import PMQConstants;

Thread.Mutex lock = Thread.Mutex();

ADT.Queue q;

mapping to_be_acked = ([]);

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

void acknowledge(string messageid)
{
werror("acknowledge: %O\n", messageid);
  if(to_be_acked[messageid])
    m_delete(to_be_acked, messageid);
}

void unacknowledge(string messageid)
{
  if(to_be_acked[messageid])
  {
    Message.PMQMessage m = to_be_acked[messageid];
    m_delete(to_be_acked, messageid);
    m->set_header("pmq-redelivered", "1");
    low_post_message(m);
  }
}

void queuesize()
{
  werror("%d messages in %s\n", sizeof((array)q), name);
  werror("%d messages to be acknowledged\n", sizeof(indices(to_be_acked)), name);
  call_out(queuesize,5);
}

int add_waiter(PMQ.PMQSSession s)
{
  if(!listeners[s])
  {
     DEBUG(2, "Session %O not already subscribed.\n");
     return 0;
  }
  if(waiters[s]) return -1;
  else
    waiters[s] = 1;

  return 1;
}

int remove_waiter(PMQ.PMQSSession s)
{
  if(!listeners[s])
  {
     DEBUG(2, "Session %O not already subscribed.\n");
     return 0;
  }
  if(waiters[s])
    waiters[s] = 0;

  return 1;
}

// a return value of 0 indicates we're already waiting for a message.
int get_message(PMQ.PMQSSession s)
{
  int res = add_waiter(s);

  call_out(process_queue, 0);

  return res;
}

// a return value of 0 indicates we're already waiting for a message.
int unget_message(PMQ.PMQSSession s)
{
  int res = remove_waiter(s);

  return res;
}

void start()
{

}

void stop()
{

}

int post_message(Message.PMQMessage message, PMQSSession session)
{
DEBUG(1, "%O->post_message(%O, %O)\n", this, message, session);
  if(writers[session])
   return low_post_message(message);
  else return 0;
  
}

int low_post_message(Message.PMQMessage message)
{
  q->write(message);
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
int st = System.gettimeofday()[1];
        if(listener->send_message(m))
        {
          m = q->read();
          if(listener->deliver_ack())
            to_be_acked[m->get_headers()["pmq-message-id"]] = m;
        }
werror("time to send message: %O\n", System.gettimeofday()[1] - st);
      }
  }

  key = 0;
}

int subscribe(PMQSSession listener)
{
 write("Queue " + name + " subscribe.\n");
  if(listener->get_mode() & MODE_LISTEN)
  {
    if(sizeof(listeners) == 0)
    {
      listeners += (< listener >);
      listener->set_queue(this);
    //  call_out(process_queue, 0);
      return CODE_SUCCESS;
    }
  }
  else if(listener->get_mode() & MODE_WRITE)
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

  if(listener->get_mode() & MODE_LISTEN)
  {
    if(sizeof(listeners) != 0 && listeners[listener])
    {
      listeners -= (< listener >);
      waiters -= (< listener >);
      return CODE_SUCCESS;
    }
  }
  else if(listener->get_mode() & MODE_WRITE)
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
