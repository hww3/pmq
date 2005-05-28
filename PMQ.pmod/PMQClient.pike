/*
 *  PMQClient: the Pike Message Queue client interface
 *
 *  Copyright (c) 2005 Bill Welliver <bill@welliver.org>
 */

import PMQ;
import PMQConstants;

PMQCConnection conn;
PMQProperties prop;
PMQIdentity identity;
string url;
int session_no;

//! create a client object. a client can be connected to one PMQ
//! server at a time, with multiple queue sessions running over it.
//! the client creates a thread to handle async i/o with the PMQ server.
//!
//! arguments supplied to the constructor will be set appropriately.
//!
//! @seealso set_identity
//! @seealso set_properties
//! @seealso set_url
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

//! sets the url of the PMQ server to connect with.
//!
//! Format: pmq[s]://host:port
void set_url(string url)
{
  this->url = url;
}

//!
void set_properties(PMQProperties prop)
{
  this->prop = prop;
}

//!
void set_identity(PMQIdentity identity)
{
  this->identity = identity;
}

//! connect to a PMQ server.
//!
//! @returns
//!  0 on failure, 1 on success.
int connect() 
{
  string host;
  int port;
  Stdio.File c = Stdio.File();

  array h = decode_url(url);
werror("%O", h);
  if(sizeof(h) == 2)
    [host, port] = h;
  else host = h[0];

  // werror("%s, %s, %d\n", url, host, port);

  if(host[0..0] == "/")
  {
    if(c->connect_unix(host)==0)
    {
      return 0;
    }
  }

  else if(!(host && port))
    return 0;

  else 
  {
    if(c->connect(host, port)==0)
    {
      return 0;
    }
  }

  conn = PMQCConnection(c, prop, identity, register_packet());

  return conn->is_running();
}

//! disconnect from a PMQ server.
//!
//! @returns
//!  0 on failure, 1 on success.
int disconnect() 
{
  if(conn)
    destruct(conn);
}

void destroy()
{
  if(conn)
    destruct(conn);
}

private mapping register_packet()
{
  mapping packets=([]);

  foreach(values(Packet), program c)
  {
    object d = c();
    if(d->type)
    {
     DEBUG(2, "startup: registering packet " + d->type + "\n");
     packets[d->type] = c;
    }
  }
  return packets;
}

private array decode_url(string url)
{
  if(search(url, ":") > 4)
    return array_sscanf(url, "pmq://%s:%d");

  else return array_sscanf(url, "pmq://%s");
}

//!
PMQQueueReader get_queue_reader(string queue)
{
  if(!conn || !conn->is_open())
    error("No connection.\n");
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
  { 
    error("wrong sessionid!\n");
  }
  if(resp->get_code() != CODE_SUCCESS)
  {
    error("subscribe failed.\n");
  }

  PMQQueueReader r = PMQQueueReader();
  PMQCSession s = PMQCSession();
  s->set_connection(conn);
  s->set_session_id(sess);

  r->set_queue(queue);
  r->set_session(s);
  return r;
}
//!
PMQTopicReader get_topic_reader(string topic)
{
  if(!conn || !conn->is_open())
    error("No connection.\n");
  Packet.PMQTSubscribe p = Packet.PMQTSubscribe();
  string sess;

  sess = generate_session_id();
DEBUG(1, "setting session id to %s\n", sess);
  p->set_topic(topic);
  p->set_mode(MODE_LISTEN);
  p->set_session(sess);

  Packet.PMQPacket resp = conn->send_packet_await_response(p);  

  if(object_program(resp) != Packet.PMQSessionResponse)
  {
    error("got invalid response to subscription request: %O\n", resp);
  }

  if(resp->get_session() != sess)
  {
    error("wrong sessionid!\n");
  }
  if(resp->get_code() != CODE_SUCCESS)
  {
    error("subscribe failed.\n");
  }

  PMQTopicReader r = PMQTopicReader();
  PMQCSession s = PMQCSession();
  s->set_connection(conn);
  s->set_session_id(sess);

  r->set_topic(topic);
  r->set_session(s);

  return r;
}

//!
PMQQueueWriter get_queue_writer(string queue)
{
  if(!conn || !conn->is_open())
    error("No connection.\n");

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

  PMQQueueWriter r = PMQQueueWriter();
  PMQCSession s = PMQCSession();
  s->set_connection(conn);
  s->set_session_id(sess);

  r->set_queue(queue);
  r->set_session(s);

  return r;
}

//!
PMQTopicWriter get_topic_writer(string topic)
{
  if(!conn || !conn->is_open())
    error("No connection.\n");

  Packet.PMQTSubscribe p = Packet.PMQTSubscribe();
  string sess;

  sess = generate_session_id();
DEBUG(1, "setting session id to %s\n", sess);
  p->set_topic(topic);
  p->set_mode(MODE_WRITE);
  p->set_session(sess);

  Packet.PMQPacket resp = conn->send_packet_await_response(p);  

  if(object_program(resp) != Packet.PMQSessionResponse)
    error("got invalid response to subscription request.\n");

  if(resp->get_session() != sess)
    error("wrong sessionid!\n");
  if(resp->get_code() != CODE_SUCCESS)
    error("subscribe failed.\n");

  PMQTopicWriter r = PMQTopicWriter();
  PMQCSession s = PMQCSession();
  s->set_connection(conn);
  s->set_session_id(sess);

  r->set_topic(topic);
  r->set_session(s);

  return r;
}

private string generate_session_id()
{
  string id;
  session_no ++;
  id = "id" + conn->client_id + "-" + session_no;
  return id;
}
