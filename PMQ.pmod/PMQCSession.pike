import PMQ;
import PMQConstants;

string session_id;

multiset listeners = (<>);
multiset writers = (<>);

PMQCConnection conn;

void create()
{

}

void set_connection(PMQCConnection conn)
{
  if(conn)
    conn->add_session(this);
  this->conn = conn;

  if(! this->conn)
  {
    destruct();
  }
}

PMQCConnection get_connection()
{  
  return this->conn;
}

void set_session_id(string session_id)
{
  this->session_id = session_id;
}

string get_session_id()
{
  return this->session_id;
}

void start()
{
  Packet.PMQStartSession p = Packet.PMQStartSession();

  p->set_session(get_session_id());

  get_connection()->send_packet(p);
}

void stop()
{
  Packet.PMQStopSession p = Packet.PMQStopSession();

  p->set_session(get_session_id());

  get_connection()->send_packet(p);
}

void deliver(Message.PMQMessage m)
{
  foreach(indices(listeners);; PMQQueueReader r)
  {
    r->deliver(m);
  }
}

void set_listener(PMQQueueReader r)
{
  listeners[r] = 1;
}

void destroy()
{
  DEBUG(4, "PMQCSession(%s)->destroy()\n", get_session_id());
  foreach(indices(listeners + writers), object o)
    o->session_abort(this);

  
}
