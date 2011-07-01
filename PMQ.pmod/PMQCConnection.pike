import PMQ;
  inherit PMQConnection;
  import PMQConstants;
int acked= 0; 
  multiset sessions = (<>);

  PMQIdentity identity;
  function reconnect_attempt_func;

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
   //werror("read buffer: %O\n", read_buffer);
   //werror("mode: %O\n", conn->mode());
    backend->call_out(stateofread, 5);
  }

  int attempt_reconnect()
  {
    if(reconnect_attempt_func)
    {
      reconnect_attempt_func(this);
      return 1;
    }
  }

/*
  int attempt_reconnect()
  {
    // things to do:
    // second, clear the state and reset any network level queues
    // third, attempt to actually reconnect periodically
    // fourth, if we reconnect, then we should resubscribe
    // and patch up the existing readers.
    object newconn;

    clear_state();
    newconn = create_connection();
    conn = newconn;
    setup_backend();
    negotiate_connection();
    renew_subscriptions();
  }
  
  void setup_backend()
  { 
    // if the backend handler thread has quit, restart it.
    if(backend_running == 0)
    {
      DEBUG(2, "setting up backend handler thread for client connection\n");
      handler = Thread.Thread(do_run_backend);
    }
  }

  void clear_state()
  {
    in_net_queue->flush(); 
    out_net_queue->flush(); 

    network_state = NETWORK_STATE_START;
    connection_state = CONNECTION_START;
  }

  // make 3 attempts then give up.
  Stdio.File create_connection()
  {
    int i = 1;
    int connected = 0;

    string addr;
    int port;
    object newconn;

    [addr, port] = array_sscanf(conn->query_address(), "%s %d");

    do
    {
      DEBUG(1, "attempting to connect to " + addr + " port " + port + "\n");
      newconn = Stdio.File();
      
      if(newconn->connect(addr, port)) connected = 1;
      i++;      
      sleep(i*5);
    } while(!connected && i <= 3);
    
    if(!connected) {
      DEBUG(1, "unable to connect to " + addr + " port " + port + "\n");
      throw(Error.Generic("unable to reconnect to pmqd; giving up.\n"));
    }
    else DEBUG(1, "successful re-connect to " + addr + " port " + port + "\n");
    return newconn;
  }

  void renew_subscriptions()
  {
  }
*/
  void create(Stdio.File conn, PMQProperties config, PMQIdentity identity, mapping packets, void|Pike.Backend b)
  {
    ::create(conn, config, packets, b);

    set_weak_flag(sessions, Pike.WEAK);
 
    this->identity = identity;
    DEBUG(4, "PMQCConnection: create!\n");

    negotiate_connection();
  }

  void negotiate_connection() 
  {
    Packet.PMQPacket r;

    r = collect_packet("welcome", 5);

    DEBUG(3, "%O->negotiate_connection(): got packet %O\n", this, r);

    if(object_program(r) == Packet.PMQSHello)
    {
      DEBUG(3, sprintf("%O: got Server Hello.\n", this));

      Packet.PMQPacket p = Packet.PMQCHello();

      string selected_version = select_version(r);

      DEBUG(2, "Selected protocol version %s\n", selected_version);
         
      if(!selected_version)
      {
        handle_protocol_error();
        return 0;
      }

      protocol_version = selected_version;

      client_id = generate_client_id(r);

      p->set_version(selected_version);    
      p->set_client_id(client_id);    
      r = send_packet_await_response(p);
      DEBUG(3, "%O->negotiate_connection(): got packet %O\n", this, r);
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
   
    if(r) return 0;
    
    if(object_program(packet) == Packet.PMQGoodbye)
    {
      DEBUG(3, sprintf("%O: got Goodbye.\n", this));
      destruct();
      return 0;
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
        return 0;
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

  sess = Crypto.MD5()->update("PMQ" + Crypto.Random.random_string(10) +  time() + gethostname() + conn->query_address())->digest();

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
