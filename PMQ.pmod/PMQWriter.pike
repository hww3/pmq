import PMQ;
import PMQConstants;

int message_no;
PMQCSession session;

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

void session_abort(PMQCSession s)
{
  // we should only allow aborting from the same session we're using.
  if(session == s)
  {
    session = 0;
  }
}

int post(Message.PMQMessage m)
{
  if(!this->session)
  {
    error("No session.\n");
    return 0;
  }
  Packet.PMQPostMessage p = Packet.PMQPostMessage();
  string message_id = generate_message_id();

  p->set_session(session->get_session_id());
  
  m->set_header("pmq-message-id", message_id);

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
