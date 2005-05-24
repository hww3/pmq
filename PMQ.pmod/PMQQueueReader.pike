import PMQConstants;

Thread.Mutex lock = Thread.Mutex();
ADT.Queue incoming = ADT.Queue();
string queue;
PMQCSession session;
function incoming_callback;
ADT.Queue incoming_queue;
int incoming_wait = 0;
Pike.Backend be = Pike.Backend();

void create()
{
  incoming_queue = ADT.Queue();
}

void deliver(Message.PMQMessage m)
{
  DEBUG(1, "PMQQueueReader: incoming %s message from %s, %s\n", 
    m->_typeof(), m->headers["sent-from"], m->headers["pmq-message-id"]);

  if(incoming_callback)
  {
    incoming_callback(m, this);
  }
  if(incoming_wait)
  {
write("putting an item in the queue for the backend.\n");
    be->call_out(incoming_queue->write, 0, m);
  }
  else
  {
    incoming_queue->write(m);
  }
}

Message.PMQMessage read(int|float|void wait)
{
  Thread.MutexKey key = lock->lock();
  incoming_wait = 1;
  // if messages are waiting for us to read, just get one.
  if(!incoming_queue->is_empty())
  {
   write("havawaitingmessage\n");
    Message.PMQMessage m = incoming_queue->read();
    incoming_wait = 0;
    key = 0;
    return m;
  }

  // otherwise, we need to wait for one.
  mixed r;
 
  if(wait)
    r = be(wait);  
  else
    r = be(3600.0);

  if(r)
  {
    Message.PMQMessage m = incoming_queue->read();
    key = 0;
    incoming_wait = 0;
    return m;
  }
 
  else
  {
write("didn'tgetawaitingmessage\n");
    incoming_wait = 0;
    key = 0;
    return 0;
  }
}

PMQCSession get_session()
{
  return this->session;
}

void set_session(PMQCSession session)
{
  this->session = session;
  session->set_listener(this);
}

string get_queue()
{
  return this->queue;
}

void set_queue(string queue)
{
  this->queue = queue;
}

