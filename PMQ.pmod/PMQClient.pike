/*
 *  PMQClient: the Pike Message Queue client interface
 *
 *  Copyright (c) 2005 Bill Welliver <bill@welliver.org>
 */

import PMQConstants;

PMQCConnection conn;
PMQProperties prop;
PMQIdentity identity;
string url;
int session_no;

void create(string|void url, PMQProperties|void prop, PMQIdentity|void identity)
{
  if(url)
  {
    set_url(url);
  }
  if(identity)
  {
    set_identity(identity);
  }
  if(prop)
  {
    set_properties(prop);
  }
}

void set_url(string url)
{
  this->url = url;
}

void set_properties(PMQProperties prop)
{
  this->prop = prop;
}

void set_identity(PMQIdentity identity)
{
  this->identity = identity;
}

int connect() 
{
  string host;
  int port;
  Stdio.File c = Stdio.File();

  [host, port] = decode_url(url);

  werror("%s, %s, %d\n", url, host, port);

  if(!(host && port))
    return 0;

  if(c->connect(host, port)==0)
  {
    return 0;
  }

  conn = PMQCConnection(c, prop, identity, register_packet());
}

private mapping register_packet()
{
  mapping packets=([]);

  foreach(values(Packet), program c)
  {
    object d = c();
    if(d->type)
    {
     write("startup: registering packet " + d->type + "\n");
     packets[d->type] = c;
    }
  }
  return packets;
}

private array decode_url(string url)
{
  return array_sscanf(url, "pmq://%s:%d");
}

PMQQueueReader get_queue_reader(string queue)
{
  Packet.PMQQSubscribe p = Packet.PMQQSubscribe();
  string sess;

  sess = generate_session_id();
DEBUG(1, "setting session id to %s\n", sess);
  p->set_queue(queue);
  p->set_mode(MODE_LISTEN);
  p->set_session(sess);

  Packet.PMQPacket resp = conn->send_packet_await_response(p);  

  if(object_program(resp) != Packet.PMQSessionResponse)
    error("got invalid response to subscription request: %O\n", resp);

  if(resp->get_session() != sess)
    error("wrong sessionid!\n");
  if(resp->get_code() != CODE_SUCCESS)
    error("subscribe failed.\n");

  PMQQueueReader r = PMQQueueReader();
  PMQCSession s = PMQCSession();
  s->set_connection(conn);
  s->set_session_id(sess);

  r->set_queue(queue);
  r->set_session(s);

  return r;
}

PMQQueueWriter get_queue_writer(string queue)
{
  Packet.PMQQSubscribe p = Packet.PMQQSubscribe();
  string sess;

  sess = generate_session_id();
DEBUG(1, "setting session id to %s\n", sess);
  p->set_queue(queue);
  p->set_mode(MODE_WRITE);
  p->set_session(sess);

  Packet.PMQPacket resp = conn->send_packet_await_response(p);  

  if(object_program(resp) != Packet.PMQSessionResponse)
    error("got invalid response to subscription request.\n");

  if(resp->get_session() != sess)
    error("wrong sessionid!\n");
  if(resp->get_code() != CODE_SUCCESS)
    error("subscribe failed.\n");

  PMQQueueReader r = PMQQueueWriter();
  PMQCSession s = PMQCSession();
  s->set_connection(conn);
  s->set_session_id(sess);

  r->set_queue(queue);
  r->set_session(s);

  return r;
}

string generate_session_id()
{
  string id;
  session_no ++;
  id = "id" + conn->client_id + "-" + session_no;
  return id;
}
