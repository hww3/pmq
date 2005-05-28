import PMQ;
import PMQConstants;

Thread.Mutex lock = Thread.Mutex();
object session;
function incoming_callback;
ADT.Queue incoming_queue;
function incoming_wait = 0;
Pike.Backend be = Pike.Backend();

void create()
{
  incoming_queue = ADT.Queue();
}

void session_abort(object s)
{
  // we should only allow aborting from the same session we're using.
  if(session == s)
  {
    session = 0;
    if(incoming_wait)
    {
      be->call_out(lambda(){ return 0;}, 0);
    }
  }
}

//!
void start()
{
  if(!incoming_callback)
  {
    error("start() called without incoming callback set.\n");
    return;
  }
  session->start();  
}

//!
void stop()
{
  session->stop();  
}

//!
void set_read_callback(function cb)
{
  incoming_callback = cb;
}

//!
function get_read_callback()
{
  return incoming_callback;
}

void deliver(Message.PMQMessage m)
{
  DEBUG(1, "PMQReader: incoming %s message from %s, %s\n", 
    m->_typeof(), m->headers["sent-from"], m->headers["pmq-message-id"]);

  if(incoming_wait)
  {
    be->call_out(incoming_wait, 0, m);
  }
  else if(incoming_callback)
  {
    incoming_callback(m, this);
  }
}

//! read a message from the queue. 
//!
//! an internal queue of messages delivered to this client is kept
//! and depleted. if there are no waiting messages to be read, then 
//! this call will block for up to wait seconds until a message arrives
//! at the client.
//!
//! delivery will not take place until the session has been started.
//!
//! @note
//! this method is multithread safe; only one thread will be executing in 
//! this method at a time. all other reads will be locked until the 
//! thread holding the lock completes the read.
//!
//! @note 
//!   the default wait time is 3600 seconds (one hour).
Message.PMQMessage read(int|float|void wait)
{
  Thread.MutexKey key = lock->lock();

  PMQ.Message.PMQMessage msg;

  incoming_wait = lambda(object m){ msg = m; };

  if(!session)
  {
    error("No session.\n");
    incoming_wait = 0;
    return 0;
  }

  session->get_message();

/*
  werror("mode before backend %O:\n", session->get_connection()->conn->mode());
*/
  mixed r;

  if(wait)
    r = be(wait);  
  else
    r = be(3600.0);

  incoming_wait = 0;
  key = 0;
  return msg;
}

object get_session()
{
  return this->session;
}

void set_session(object session)
{
  this->session = session;
  session->set_listener(this);
}
