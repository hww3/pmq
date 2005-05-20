  inherit PMQConnection;
  import PMQConstants;
  
  PMQIdentity identity;

  multiset connections = (<>);
  multiset write_sessions = (<>);
  multiset listen_sessions = (<>);

  PMQSSession get_session_by_id(string session_id, int mode)
  {
    mixed sess;

    if(mode == MODE_LISTEN)
    {
      foreach(indices(listen_sessions);;PMQSSession s)
        if(s->get_session_id() == session_id) return s;
    }
    if(mode == MODE_WRITE)
    {
      foreach(indices(write_sessions);;PMQSSession s)
        if(s->get_session_id() == session_id) return s;
    }

    return 0;
  }

  void create(Stdio.File conn, PMQProperties config, multiset connections, mapping packets)
  {
    this->connections = connections;

    ::create(conn, config, packets);
    write("PMQSConnection: create!\n");
    backend->call_out(shello, 0);
//    call_out(done, 5);
  }

  PMQIdentity get_identity()
  {
    return identity;
  }

  void set_identity(PMQIdentity identity)
  {
    this->identity = identity;
  }

  void shello()
  {
    Packet.PMQSHello packet = Packet.PMQSHello();
    packet->set_versions(({"1.0", "2.0"}));
    send_packet(packet, 1);
    connection_state = CONNECTION_SENT_SHELLO;
  }

  int send_message(Message.PMQMessage message, PMQSSession session)
  {
    Packet.PMQDeliverMessage p = Packet.PMQDeliverMessage();
    string queue = session->get_queue()->name;
    string s = session->get_session_id();

    message->set_header("Sent-From", queue);
    message->set_header("Session", s);

DEBUG(5, "Message: %s\n", (string)message);

    p->set_pmqmessage(message);

    Packet.PMQPacket r = send_packet_await_response(p);
if(r)
  DEBUG(1, "got response from client.\n");
else
  DEBUG(1, "got nothing from client.\n");

    if(!r)
    {
      write("trying to wake up connection..." + conn->query_address() + "\n");
      return 0;
    }

    if(object_program(r) != Packet.PMQAck)
    {
      if(r) remote_read("", (string)r);
      return 0;
    }
    if(r->get_id() != message->headers["pmq-message-id"])
    {
      DEBUG(2, "got PMQAck for wrong message id: %s, got %s\n",
         message->headers["pmq-message-id"], r->get_id());
      return 0;
    }
    return 1;
  }

  //
  // this is the state machine for received packets. 
  // this is where it all happens.
  //
  void handle_packet(Packet.PMQPacket packet)
  {
    write(sprintf("handle_packet: got %O in state %d\n", packet, connection_state));
    // we can get a goodbye at any time.
    if(object_program(packet) == Packet.PMQGoodbye)
    {
      destruct();
      return;
    }

    else if(object_program(packet) == Packet.PMQNoOp)
    {
      write(sprintf("%O: got NoOp.\n", this));
    }

    else if(connection_state == CONNECTION_RUNNING)
    {
      if(object_program(packet) == Packet.PMQQSubscribe)
      {
        set_network_mode(MODE_BLOCK);
        string queue_name = packet->get_queue();

        PMQSSession session = PMQSSession();
        session->set_session_id(packet->get_session());
        session->set_connection(this);
        session->set_mode(packet->get_mode());

        Queue.PMQQueue q = manager->get_queue_by_name(queue_name);
        if(!q)
        {
          q = manager->new_queue(queue_name, "PMQQueue");
        }

        Packet.PMQSessionResponse response = Packet.PMQSessionResponse();
        response->set_session(session->get_session_id());


        if(q)
        {
          if(q->subscribe(session))
          {
            if(session->get_mode() == MODE_LISTEN)
              listen_sessions[session] = 1;
            if(session->get_mode() == MODE_WRITE)
              write_sessions[session] = 1;  
            response->set_code(CODE_SUCCESS);
          }
          else
          {
            response->set_code(CODE_FAILURE);
          }
          send_packet(response, 1);
          set_network_mode(MODE_NONBLOCK);
        }
      }

      if(object_program(packet) == Packet.PMQQUnsubscribe)
      {
        string queue_name = packet->get_queue();

        PMQSSession s = get_session_by_id(packet->get_session(), packet->get_mode());

        Queue.PMQQueue q = manager->get_queue_by_name(queue_name);

        if(q)
        {
          if(q->unsubscribe(s))
          {
            if(s->get_mode() == MODE_LISTEN)
              listen_sessions[s] = 0;
            if(s->get_mode() == MODE_WRITE)
              write_sessions[s] = 0;
           }
        }
      }

      if(object_program(packet) == Packet.PMQPostMessage)
      {
        set_network_mode(MODE_BLOCK);
        string queue_name = packet->get_queue();
        string sid = packet->get_session();
        Queue.PMQQueue q = manager->get_queue_by_name(queue_name);

        if(!q)
        {
          q = manager->new_queue(queue_name, "PMQQueue");
        }

        Message.PMQMessage m = packet->get_pmqmessage();
        Packet.PMQPacket r = Packet.PMQAck();
        r->set_id(m->get_header("pmq-message-id"));
        r->set_code(0);

        if(q)
        {
          PMQSSession s = get_session_by_id(sid, MODE_WRITE);
          
          int pr = q->post_message(m, s);

          r->set_code(pr);
          
        }

        send_packet(r, 1);       
        set_network_mode(MODE_NONBLOCK);     
        return;
      }

    }

    else if(connection_state == CONNECTION_SENT_SHELLO)
    {
      if(object_program(packet) == Packet.PMQCHello)
      {
        protocol_version = packet->get_version();
        client_id = packet->get_client_id();

        if(!client_id || !protocol_version)
        {
          handle_protocol_error();
          return;
        }
       
        set_network_mode(MODE_BLOCK);

        backend->call_out(handle_auth, 0, packet);
        return;
      }
      else  // we can only have a client hello here
      {
        handle_protocol_error();
        return;
      }
    }

    else
    {
       handle_protocol_error();
       return;
    }

  }

  void handle_auth(Packet.PMQCHello packet)
  {
    write("handle_auth\n");

    backend->call_out(con_running, 0);
  }

  void con_running()
  {
    Packet.PMQWelcome p = Packet.PMQWelcome();
    p->set_client_id(client_id);
    send_packet(p, 1);
    connection_state = CONNECTION_RUNNING;
    set_network_mode(MODE_NONBLOCK);
  }

  void destroy()
  {
    foreach(indices(listen_sessions);; PMQSSession s)
      s->get_queue()->unsubscribe(s);    
    ::destroy();
    connections[this] = 0;
  }
