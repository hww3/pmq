import PMQ;
import PMQConstants;

Thread.Mutex lock = Thread.Mutex();
//array session;
object session;
function incoming_callback;
ADT.Queue incoming_queue;
function incoming_wait = 0;
Pike.Backend be = Pike.Backend();

constant requires_get = 1;

void create()
{
//  session = allocate(1);
  incoming_queue = ADT.Queue();
//  set_weak_flag(session, Pike.WEAK);
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
/*

  if(!incoming_callback)
  {
    error("start() called without incoming callback set.\n");
    return;
  }
  session->start();  
*/
incoming_wait = lambda(object m){ incoming_queue->write(m); };

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
//werror("deliver called %O\n", System.gettimeofday()[1] - st);
  if(incoming_wait)
  {
    be->call_out(incoming_wait, 0, m);
  }
  else if(incoming_callback)
  {
    incoming_callback(m, this);
  }
//werror("deliver finished %O\n", System.gettimeofday()[1] - st);
}

//! returns a flag that indicates whether there are messages
//! already delivered and waiting to be read.
int have_messages()
{
  return !incoming_queue->is_empty();
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
int st = 0;

Message.PMQMessage read(int|float|void wait)
{
 st = System.gettimeofday()[1];
//werror("locking. %O\n", System.gettimeofday()[1]-st);
  Thread.MutexKey key = lock->lock();

//  PMQ.Message.PMQMessage msg;

  if(!incoming_queue->is_empty()) 
  {
//werror("queue has items!\n");
    object m;
    m = incoming_queue->read();
    key = 0;
    return m;
  }  
if(!incoming_wait)incoming_wait = lambda(object m){ incoming_queue->write(m); };

  if(!session)
  {
    error("No session.\n");
    incoming_wait = 0;
    return 0;
  }
//werror("getting message %O\n", System.gettimeofday()[1]-st);
if(requires_get)
  session->get_message();
//werror("got message %O\n", System.gettimeofday()[1]-st);

/*
  werror("mode before backend %O:\n", session[0]->get_connection()->conn->mode());
*/
  mixed r;
do
{

//werror("waiting %O\n", System.gettimeofday()[1]-st);
  if(wait)
    r = be(wait/2);  
  else
    r = be(1800.0);
}while(incoming_queue->is_empty());
//werror("done waiting %O\n", System.gettimeofday()[1]-st);
object mesg = incoming_queue->read();
//  incoming_wait = 0;
  key = 0;
st = 0;
  return mesg;
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

void destroy()
{
  destruct(session);
}
