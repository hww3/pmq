import PMQ;
  inherit PMQConnection;
  import PMQConstants;
int acked= 0; 
  multiset sessions = (<>);

  PMQIdentity identity;

  int get_mode()
  {
    return MODE_CLIENT;
  }

  PMQCSession get_session_by_id(string session_id)
  {
    foreach(indices(sessions);; PMQCSession s)
    {
      if(s->get_session_id() == session_id)
        return s;
    }
    return 0;
  }

void stateofread()
{
   werror("read buffer: %O\n", read_buffer);
   werror("mode: %O\n", conn->mode());
    backend->call_out(stateofread, 5);

}
  void create(Stdio.File conn, PMQProperties config, PMQIdentity identity, mapping packets, void|Pike.Backend b)
  {
    ::create(conn, config, packets, b);

    Packet.PMQPacket r;

    this->identity = identity;
    DEBUG(4, "PMQCConnection: create!\n");
    r = collect_packet("welcome", 5);

    DEBUG(3, "%O->create(): got packet %O\n", this, r);

    if(object_program(r) == Packet.PMQSHello)
    {
      DEBUG(3, sprintf("%O: got Server Hello.\n", this));

      Packet.PMQPacket p = Packet.PMQCHello();

      string selected_version = select_version(r);

      DEBUG(2, "Selected protocol version %s\n", selected_version);
         
      if(!selected_version)
      {
        handle_protocol_error();
        return;
      }

      protocol_version = selected_version;

      client_id = generate_client_id(r);

      p->set_version(selected_version);    
      p->set_client_id(client_id);    
      r = send_packet_await_response(p);
      DEBUG(3, "%O->create(): got packet %O\n", this, r);
      connection_state = CONNECTION_SENT_CHELLO;

     
    }
    else
    {
      DEBUG(1, "handshake error!\n");
      handle_protocol_error();
    }

    if(object_program(r) == Packet.PMQWelcome)
    {
      DEBUG(3, sprintf("%O: got Welcome.\n", this));
      connection_state = CONNECTION_RUNNING;
    }
    else
    {
      handle_protocol_error();
      return;
    }

    set_conn_callbacks_nonblocking();

  }

  void quit()
  {
    DEBUG(3, "closing connection.\n");
    send_packet(Packet.PMQGoodbye());
    destruct();
  }

  void destroy()
  {
    foreach(indices(sessions), PMQCSession s)
    {
      s->set_connection(0);
    }

    ::destroy();
   
    DEBUG(4, "PMQCConnection: destroy\n");
    // send_packet(Packet.PMQGoodbye());
  }

  void add_session(PMQCSession s)
  {
    if(!sessions[s])
      sessions[s] = 1;
  }

  void del_session(PMQCSession s)
  {
    if(sessions[s])
      sessions[s] = 0;
  }

  void handle_packet(Packet.PMQPacket packet)
  {
    DEBUG(3, "handle_packet(%O)\n", packet);

    int r = ::handle_packet(packet);
   
    if(r) return;

    if(object_program(packet) == Packet.PMQGoodbye)
    {
      DEBUG(3, sprintf("%O: got Goodbye.\n", this));
      destruct();
      return;
    }
    else if(connection_state == CONNECTION_RUNNING)
    {
      if(object_program(packet) == Packet.PMQDeliverMessage)
      {
        Message.PMQMessage m = packet->get_pmqmessage();
        PMQCSession sess = get_session_by_id(m->headers["session"]);
        if(!sess) werror( "Misdelivered message for session %O\n", 
                        m->headers);
        sess->deliver(m, packet->get_id());
        return;
      }

      if(object_program(packet) == Packet.PMQSessionResponse)
      {
        DEBUG(3, sprintf("%O: got SessionResponse: %d.\n", this, 
          packet->get_code()));
      }
    }
    else if(connection_state == CONNECTION_SENT_CHELLO)
    {
      write("*** we shouldn't have gotten here!\n");
    }
    else if(connection_state == CONNECTION_START)
    {
      write("*** we shouldn't have gotten here!\n");
    }

  }


string generate_client_id(Packet.PMQSHello packet)
{
  string sess = "";

  sess = Crypto.MD5()->update("PMQ" + time() + gethostname() + conn->query_address())->digest();

  sess = String.string2hex(sess);

  return sess;
}

string select_version(Packet.PMQSHello packet)
{
  if(search(packet->get_versions(), "1.0") == -1)
  {
    return 0;
  }
  else return "1.0";
}
