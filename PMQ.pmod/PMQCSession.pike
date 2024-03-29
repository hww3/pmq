import PMQ;
import PMQConstants;

int mode;
string session_id;
int stopped = 0;

multiset listeners = (<>);
multiset writers = (<>);

array conn;

void create()
{
 conn = allocate(1);
 set_weak_flag(conn, Pike.WEAK);
 set_weak_flag(listeners, Pike.WEAK);
 set_weak_flag(writers, Pike.WEAK);
}

void set_mode(int m)
{
  mode = m;
}

int get_mode()
{  
  return mode;
}

int deliver_ack()
{
  return (mode & MODE_DELIVER_ACK);
}

int submit_ack()
{
  return (mode & MODE_SUBMIT_ACK);
}

void set_connection(PMQCConnection conn)
{
  if(conn)
  {
    conn->add_session(this);
    this->conn[0] = conn;
  }
  else 
  {
    destruct();
  }
}

PMQCConnection get_connection()
{  
  return this->conn[0];
}

void set_session_id(string session_id)
{
  this->session_id = session_id;
}

string get_session_id()
{
  return this->session_id;
}

void unsubscribe()
{
  foreach(listeners + writers; object o;)
  {
    if(o->get_queue)
    {
      Packet.PMQQUnsubscribe p = Packet.PMQQUnsubscribe();
  
      p->set_queue(o->get_queue());
      p->set_session(get_session_id());
  
      conn[0]->send_packet(p);

    }
    else if(o->get_topic)
    {
      Packet.PMQTUnsubscribe p = Packet.PMQTUnsubscribe();
  
      p->set_topic(o->get_topic());
      p->set_session(get_session_id());
  
      conn[0]->send_packet(p);
   
    }
  }

}

void start()
{
  if(!stopped) return;

  Packet.PMQStartSession p = Packet.PMQStartSession();

  p->set_session(get_session_id());

  get_connection()->send_packet(p);
  stopped = 0;
}

void get_message()
{
  Packet.PMQGetMessage p = Packet.PMQGetMessage();

  p->set_session(get_session_id());

  get_connection()->send_packet(p);
}

void stop()
{
  if(stopped) return;

  Packet.PMQStopSession p = Packet.PMQStopSession();

  p->set_session(get_session_id());
 
  if(get_connection())
    get_connection()->send_packet(p);
  stopped = 1;
}

void deliver(Message.PMQMessage m, mixed reply_id)
{
  foreach((listeners); PMQQueueReader r;)
  {
    r->deliver(m);
  }
  if(deliver_ack())
  {
    get_connection()->backend->call_out(ack_delivery, 0, 
       m->headers["pmq-message-id"], reply_id);
//    ack_delivery(
//       m->headers["pmq-message-id"], reply_id);
  }
}

void set_listener(PMQQueueReader r)
{
  listeners[r] = 1;
}

void destroy()
{
  DEBUG(4, "PMQCSession(%s)->destroy()\n", get_session_id());
  foreach(listeners + writers; object o;)
    o->session_abort(this);

  stop();
  unsubscribe();
}

void ack_delivery(string message_id, string reply_id)
{
           Packet.PMQAck a = Packet.PMQAck();
           a->set_id(message_id);
           a->set_session(session_id);
           a->set_reply_id(reply_id);
           get_connection()->send_packet(a);
}
