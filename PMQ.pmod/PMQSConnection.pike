  import PMQ;
  inherit PMQConnection;
  import PMQConstants;
  
  PMQIdentity identity;

  multiset connections = (<>);
  multiset write_sessions = (<>);
  multiset listen_sessions = (<>);

int st;
  int shello_co;

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
  }

  void go()
  {
    set_conn_callbacks_nonblocking();
    backend->call_out(shello, 0);
    shello_co = backend->call_out(shello_timeout, 5);

  }

  void stillalive()
  {
    conn->query_address();
    backend->call_out(stillalive, 5);
  }

  void shello_timeout()
  {
    destruct(this);
//    backend->call_out(shello, 0);
//    shello_co = backend->call_out(shello_timeout, 5);
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
    DEBUG(1, "sending SHello.\n");
    Packet.PMQSHello packet = Packet.PMQSHello();
    packet->set_versions(({"1.0", "2.0"}));
    packet->set_reply_id("welcome");
    send_packet(packet);
    connection_state = CONNECTION_SENT_SHELLO;
  }

  int send_message(Message.PMQMessage message, PMQSSession session, int ack)
  {
werror("send_message %O\n", System.gettimeofday()[0] - st);
    Packet.PMQDeliverMessage p = Packet.PMQDeliverMessage();
    string queue = session->get_queue()->name;
    string s = session->get_session_id();
    message = message->clone();
    message->set_header("Sent-From", queue);
    message->set_header("Session", s);

DEBUG(5, "Message: %s\n", (string)message);

    p->set_pmqmessage(message);

     if(catch(send_packet(p)))
       return 0;
     else
       return 1;
  }

  //
  // this is the state machine for received packets. 
  // this is where it all happens.
  //
  void handle_packet(Packet.PMQPacket packet)
  {
DEBUG(1, "%O->handle_packet(%O)\n", this, packet);

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

    int r = ::handle_packet(packet);

    if(r) return;

    else if(connection_state == CONNECTION_RUNNING)
    {
      if(object_program(packet) == Packet.PMQAck)
      {
        PMQSSession s = get_session_by_id(packet->get_session(), MODE_LISTEN);

        if(!s)
        {
          werror("PMQSConnection: unknown session in start command!\n");
          return;
        }

        s->acknowledge(packet->get_id());
      }
      else if(object_program(packet) == Packet.PMQStartSession)
      {
        PMQSSession s = get_session_by_id(packet->get_session(), MODE_LISTEN);

        if(!s) 
        {
          werror("PMQSConnection: unknown session in start command!\n");
          return;
        }
       
        s->start();

      }

      if(object_program(packet) == Packet.PMQStopSession)
      {
        PMQSSession s = get_session_by_id(packet->get_session(), MODE_LISTEN);

        if(!s) 
        {
          werror("PMQSConnection: unknown session in stop command!\n");
          return;
        }
       
        s->stop();

      }

      if(object_program(packet) == Packet.PMQGetMessage)
      {
st = System.gettimeofday()[1];
werror("got packet %O\n", System.gettimeofday()[1] - st);
        PMQSSession s = get_session_by_id(packet->get_session(), MODE_LISTEN);
werror("got session %O\n", System.gettimeofday()[1] - st);

        if(!s) 
        {
          werror("PMQSConnection: unknown session in getmessage command!\n");
          return;
        }
       
werror("getting message %O\n", System.gettimeofday()[1] - st);
        s->get_message();
werror("done getting message %O\n", System.gettimeofday()[1] - st);

      }

      if(object_program(packet) == Packet.PMQQSubscribe || 
         object_program(packet) == Packet.PMQTSubscribe)
      {
        int type = 0;
        if(object_program(packet) == Packet.PMQTSubscribe)
          type = 1;

        string queue_name;
        if(type)
          queue_name = packet->get_topic();
        else queue_name = packet->get_queue();

        PMQSSession session = PMQSSession();
        session->set_session_id(packet->get_session());
        session->set_connection(this);
        session->set_mode(packet->get_mode());

        Queue.PMQQueue q;
        if(!manager) werror("*** QUEUE MANAGER MISSING!\n");
        if(type) q = manager->get_topic_by_name(queue_name);
        else q = manager->get_queue_by_name(queue_name);

        if(!q && type && config->get("pmqd.topic.autocreate") == "1")
        {
          q = manager->new_topic(queue_name, "PMQSimpleTopic");
        }

        if(!q && !type && config->get("pmqd.queue.autocreate") == "1")
        {
          q = manager->new_queue(queue_name, "PMQSimpleQueue");
        }

        Packet.PMQSessionResponse response = Packet.PMQSessionResponse();
        response->set_session(session->get_session_id());
        response->set_reply_id(packet->get_id());

        int res = 0;

        if(q)
        {
          res = q->subscribe(session);

          if(res == CODE_SUCCESS)
          {
            if(session->get_mode() & MODE_LISTEN)
              listen_sessions[session] = 1;
            if(session->get_mode() & MODE_WRITE)
              write_sessions[session] = 1;  
            response->set_code(CODE_SUCCESS);
          }
          else
          {
            response->set_code(res);
          }
        }
        else
        {
            response->set_code(CODE_NOTFOUND);
        }
          send_packet(response);
      }

      if(object_program(packet) == Packet.PMQQUnsubscribe ||
         object_program(packet) == Packet.PMQTUnsubscribe)
      {
        int type = 0;
        if(object_program(packet) == Packet.PMQTSubscribe)
          type = 1;

        string queue_name;
        if(type)
          queue_name = packet->get_topic();
        else queue_name = packet->get_queue();

        PMQSSession s;
        s = (get_session_by_id(packet->get_session(), MODE_LISTEN) 
               || get_session_by_id(packet->get_session(), MODE_WRITE));

        Queue.PMQQueue q;

        if(s && s->queue)
          q = s->queue; 

        if(q)
        {
          if(q->unsubscribe(s) == CODE_SUCCESS)
          {
            if(s->get_mode() & MODE_LISTEN)
              listen_sessions[s] = 0;
            if(s->get_mode() & MODE_WRITE)
              write_sessions[s] = 0;
           }
        }
      }

      if(object_program(packet) == Packet.PMQPostMessage)
      {
        Packet.PMQAck r;

        string sid = packet->get_session();
        PMQSSession s = get_session_by_id(sid, MODE_WRITE);

        Queue.PMQQueue q = s->get_queue();

        Message.PMQMessage m = packet->get_pmqmessage();

        if(s->submit_ack())
        {
          r = Packet.PMQAck();
          r->set_reply_id(packet->get_id());
          r->set_session(sid);
          r->set_id(m->get_header("pmq-message-id"));
          r->set_code(CODE_FAILURE);
        }

        if(q)
        {
          int pr = q->post_message(m, s);

          if(s->submit_ack())
          {
            r->set_code(pr);
          }
        }

        if(s->submit_ack())
        {
          send_packet(r);
        }
      }

    }

    else if(connection_state == CONNECTION_SENT_SHELLO)
    {
      if(object_program(packet) == Packet.PMQCHello)
      {
        if(shello_co)
        {
          backend->remove_call_out(shello_co);
        }
        protocol_version = packet->get_version();
        client_id = packet->get_client_id();

        if(!client_id || !protocol_version)
        {
          handle_protocol_error();
          return;
        }
       
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
    identity = packet->get_identity();
    write(sprintf("handle_auth: %O\n", identity));

    backend->call_out(con_running, 0, packet->get_id());
  }

  void con_running(string id)
  {
    Packet.PMQWelcome p = Packet.PMQWelcome();
    p->set_client_id(client_id);
    p->set_reply_id(id);
    send_packet(p);
    connection_state = CONNECTION_RUNNING;
  }

  void destroy()
  {
    foreach(indices(listen_sessions);; PMQSSession s)
      s->get_queue()->unsubscribe(s);    
    ::destroy();
    connections[this] = 0;
  }
