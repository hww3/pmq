import PMQConstants;

ADT.Queue incoming = ADT.Queue();
string queue;
PMQCSession session;

void create()
{

}

void deliver(Message.PMQMessage m)
{
  DEBUG(1, "PMQQueueReader: incoming %s message from %s, %s\n", 
    m->_typeof(), m->headers["sent-from"], m->headers["pmq-message-id"]);
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

