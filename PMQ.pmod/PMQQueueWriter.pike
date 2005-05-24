import PMQConstants;

ADT.Queue incoming = ADT.Queue();
int message_no;
PMQCSession session;
string queue;

void create()
{

}

PMQCSession get_session()
{
  return this->session;
}

void set_session(PMQCSession session)
{
  this->session = session;
}

string get_queue()
{
  return this->queue;
}

void set_queue(string queue)
{
  this->queue = queue;
}


int post(Message.PMQMessage m)
{
  Packet.PMQPostMessage p = Packet.PMQPostMessage();
  string message_id = generate_message_id();

  p->set_queue(get_queue());
  p->set_session(session->get_session_id());
  
  m->set_header("PMQ-Message-Id", message_id);

  p->set_pmqmessage(m);

  if(0)
//  if(session->get_connection()->config->get_parameter("ack_posts"))
  {
  
  Packet.PMQPacket r = session->get_connection()->send_packet_await_response(p);

  if((object_program(r) != Packet.PMQAck) ||  
    r->get_id() != message_id) 
  { 
    session->get_connection()->handle_protocol_error(); 
    return 0; 
  } 
 
  else return r->get_code(); 
  }

  else
  {
    if(catch(session->get_connection()->send_packet(p)))
      return 0;
    else
      return 1;
  }   
}

string generate_message_id()
{
  message_no++;

  string id = session->get_session_id();
  id = id + "-" + message_no;
  return id;
}
