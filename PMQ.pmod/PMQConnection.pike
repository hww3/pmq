  import PMQ;
  import PMQConstants;

  constant NETWORK_STATE_START = 0;
  constant NETWORK_STATE_LOOKSTART = 1;
  constant NETWORK_STATE_LOOKCOMPLETE = 2;
  constant CONNECTION_START = 0;
  constant CONNECTION_SENT_SHELLO = 1;
  constant CONNECTION_SENT_CHELLO = 2;
  constant CONNECTION_RUNNING = 3;
  constant CONNECTION_RECIEVED_CHELLO = 4;
  constant CONNECTION_DISCONNECT = 255;
  constant MODE_CLIENT = 1;
  constant MODE_SERVER = 2;

  ADT.Queue out_net_queue = ADT.Queue();
  ADT.Queue in_net_queue = ADT.Queue();
  object manager;
  Stdio.File conn;
  Pike.Backend backend;
  Thread.Thread handler;
  PMQProperties config;
  int net_mode = MODE_NONBLOCK;
  string read_buffer = "";
  int network_state = NETWORK_STATE_START;
  int connection_state = CONNECTION_START;
  int look_len = 0;
  int packet_num = 0;
  int last_error = 0;
  string client_id = 0;
  string protocol_version = 0;
  int block_read_timeout = 10;

  mapping packets;
  string _sprintf(mixed ... args)
  {
    string addr;

    if(catch(addr = conn->query_address()))
      return "PMQConnection(Not Connected)";
    else if(client_id && protocol_version)
      return "PMQConnection(" + client_id + "," + protocol_version + ")";
    else
      return "PMQConnection(" + addr + ", " + connection_state + ")";
  }

  int is_running()
  {
    if(!conn->is_open)
     return 0;

    if(connection_state==CONNECTION_RUNNING)    
      return 1;

    else return 0;
  }

  int is_open()
  { 
    return conn->is_open();
  }

  void run_backend()
  {
    do
    {
      float r;
      mixed e;
      if(e = catch(
        r = backend(5.0)))
        {
          werror("***\n*** Caught an error in the backend!\n***\n");
          master()->desribe_error(e);
        }
    } while(this);
  }

  void create(Stdio.File conn, PMQProperties config, mapping packets)
  {
    this->conn = conn;
    this->config = config;
    this->packets = packets;
    network_state = NETWORK_STATE_LOOKSTART;

    if(get_mode() == MODE_CLIENT)
    {
      backend = Pike.Backend();
      handler = Thread.Thread(run_backend);
      backend->call_out(conn->set_blocking, 0);
//      DEBUG(2, "starting client backend thread\n");
    }
    else 
    {
//      backend = Pike.DefaultBackend;
      backend = Pike.Backend();
      handler = Thread.Thread(run_backend);
      set_conn_callbacks_nonblocking();
    }

  }

  void set_conn_callbacks_nonblocking()
  {
    conn->set_backend(backend);
//    backend->call_out(conn->set_backend, 0, backend);
    conn->set_nonblocking(remote_read, UNDEFINED, remote_close);
  }

  string timeout_read (Stdio.File fd, int len, int timeout)
  {
    string res = "";
    Pike.Backend be = Pike.Backend();
    int end_time = time() + timeout;
    int is_blocking = 0;
    Pike.Backend o_be;
    function r_cb, w_cb, c_cb;
   
    is_blocking = !(fd->mode() & 0x0400);

    o_be = fd->query_backend();
    r_cb = fd->query_read_callback();
    c_cb = fd->query_close_callback();
    w_cb = fd->query_write_callback();

    fd->set_backend(be);
    fd->set_nonblocking(lambda (mixed dummy, string s) {res += s;},
                        0,
                        lambda () {end_time = 0; remote_close(); });

    while (time() < end_time)
    {
      be ((float)(end_time - time()));

      if(sizeof(res) && len == UNDEFINED)
        break;
      if(len && sizeof(res) >= len) 
      {
        break;
      }
    }

    fd->set_backend(o_be);
    
    if(is_blocking)
    {
      fd->set_blocking();
    }
    else
    {
      fd->set_nonblocking(r_cb, w_cb, c_cb);
    }

    return res;
  }


  int get_mode()
  {
    return MODE_SERVER;
  }

  void set_queue_manager(PMQQueueManager manager)
  { 
    this->manager = manager;
  }

  void remote_close()
  {
    connection_state = CONNECTION_DISCONNECT;
    DEBUG(4, "remote close\n");
    destruct();    
  }

  void remote_read(string id, string data)
  {
    read_buffer+=data;

    DEBUG(5, "%O->remote_read(%O, %O)\n", this, id, data);
    if(net_mode == MODE_NONBLOCK)
    {
      read_loop();
    }
    else werror("mode is block, so we wait.\n");
  }

  void read_loop()
  {
DEBUG(5, "read_loop()\n");
    int n, my_look_len;
    string packet_data="";

    if(network_state == NETWORK_STATE_LOOKCOMPLETE)
    {

      if(sizeof(read_buffer) < look_len)
      {
        DEBUG(5, "read_loop(): not enough in buffer\n");
        return;
      }
      if(sizeof(read_buffer) == look_len)
      {
         packet_data = read_buffer;
         read_buffer = "";
        DEBUG(5, "read_loop(): got just enough.\n");
      }

      else if(sizeof(read_buffer) > look_len)
      {
        DEBUG(5, "read_loop(): more than enough in buffer\n");
        packet_data = read_buffer[0.. (look_len-1)];
        read_buffer = read_buffer[look_len ..];
      }

      if(sizeof(packet_data)) parse_packet(packet_data, 0);
      if(!this) return;
      network_state = NETWORK_STATE_LOOKSTART;
      look_len = 0;
      
      if(sizeof(read_buffer))                
        backend->call_out(read_loop, 0);
      return;
    }

    else if(network_state == NETWORK_STATE_LOOKSTART)
    {
        DEBUG(5, "read_loop(): looking for a packet.\n");

      if(sizeof(read_buffer) < 7) return; // we must have at least 7 bytes

      // PMQ plus payload len must be at the beginning of our buffer.
      n = sscanf(read_buffer, "PMQ%4c%s", my_look_len, read_buffer);

      if(n == 0) // we have a protocol error.
      {
	handle_protocol_error();
      }

      else  // we have a valid packet header.
      {
        DEBUG(5, "read_loop(): have a valid packet header\n");

        look_len = my_look_len;
        packet_data = "";
        network_state = NETWORK_STATE_LOOKCOMPLETE;
        read_loop();
      }
    } 
  } 

void handle_protocol_error()
{
  DEBUG(3, "have a malformed packet\n");
  last_error = packet_num;
  send_packet(Packet.PMQBadPacket());
//  conn->close();
//  if(this)
//    destruct();
}

  void done()
  {
    connection_state = CONNECTION_DISCONNECT;
    send_packet(Packet.PMQGoodbye());
    conn->close();
    destruct();
  }

  void|Packet.PMQPacket parse_packet(string packet_data, int mode)
  {
    string packet_type;
    string packet_payload;
    Packet.PMQPacket packet;
DEBUG(3, "parse_packet(%d)\n", sizeof(packet_data));
    packet_num++;
    int n, len;

    n = sscanf(packet_data, "%c%s", len, packet_data);
    n = sscanf(packet_data, "%" + len + "s%s", packet_type, packet_payload);

    DEBUG(5, "parse_packet: %s, %O\n", packet_type, packet_payload);

    if(!packets[packet_type])
    {
        DEBUG(3, "packet type not available.\n");
	handle_protocol_error();
	return;
    }

    else packet = packets[packet_type]();

    DEBUG(5, "parse_packet: created a packet %O\n", packet);

    packet->parse(packet_payload);
 if(0)   
 {
      DEBUG(3, "got an error parsing packet.\n");
      handle_protocol_error();
      return;
    }
    else
    {
      DEBUG(5, "what to do about the packet? %O\n", mode);
      if(mode)
        return packet; 
      else
        backend->call_out(handle_packet, 0, packet);
      return;
    }
  }

  void handle_packet(Packet.PMQPacket packet, int|void immediate)
  {
    DEBUG(4, sprintf("%O->handle_packet(%O)\n", this, packet));
  }

  void send_packet(Packet.PMQPacket packet, int|void immediate)
  {
    if(!conn->is_open())
    {
      DEBUG(3, "closing conn\n");
      conn->close();
    }
    if(net_mode == MODE_BLOCK && ! immediate)
      out_net_queue->write(packet);
    else if(this->conn)
    {
      int written = 0;
      string pkt = (string)packet;
      int towrite = strlen(pkt);

      DEBUG(4, sprintf("%O->send_packet(%O)\n", this, packet));
      conn->set_blocking();
      do
      {
        written += conn->write(pkt);
      }
      while(written<towrite);

      if(net_mode == MODE_NONBLOCK) set_conn_callbacks_nonblocking();
    }
    else DEBUG(1, "no conn!\n");
  }


  void set_network_mode(int mode)
  {
    if(mode == MODE_NONBLOCK)
    {
      if(!out_net_queue->is_empty())
      {
        do
        {
DEBUG(2, "catching up with queued outgoing packets.\n");
          send_packet(out_net_queue->read(), 1);
        }
        while(!out_net_queue->is_empty());
      }

      if(!in_net_queue->is_empty())
      {
        do
        {
DEBUG(2, "catching up with queued incoming packets.\n");
          handle_packet(out_net_queue->read(), 1);
        }
        while(!in_net_queue->is_empty());
        if(sizeof(read_buffer))
          read_loop();
      }
      net_mode = MODE_NONBLOCK;

      if(strlen(read_buffer)) backend->call_out(read_loop, 0);
    }

    else
    {
       net_mode = MODE_BLOCK;
       conn->set_blocking();
    }
  }

  Packet.PMQPacket send_packet_await_response(Packet.PMQPacket packet, 
          int|void keep)
  {
    set_network_mode(MODE_BLOCK);
    int n, my_look_len;

    if(!conn->is_open())
    {
      conn->close();
    }

    if(this->conn)
    {
      string dta;
      DEBUG(4, sprintf("%O->send_packet_await_response(%O)\n", this, packet));
      send_packet(packet, 1);//      dta = conn->read(7);
//      dta = timeout_read(conn, 7, 5);

      do
      {
        dta = conn->read(7);
DEBUG(6, "Read %O, %O from conn\n", dta, conn->errno());
      } while (!dta && conn->is_open());

DEBUG(5, "Read from conn: %O\n", dta);
      if(!dta || sizeof(dta) < 7)
      {
        DEBUG(2, "unexpected response from remote: %O.\n", dta);
        DEBUG(2, "error: %O.\n", conn->errno());

     if(!keep)
     {
       set_network_mode(MODE_NONBLOCK);
       
       return 0;
      }
      else return 0;
      }
      // PMQ plus payload len must be at the beginning of our buffer.
      n = sscanf(dta, "PMQ%4c%s", my_look_len, dta);
   
      if(!n)
      {
        if(!keep)
          set_network_mode(MODE_NONBLOCK);
        error("unexpected response from server.\n");
      }
      string newdta="";
      if(sizeof(dta) < my_look_len)
      {
//        dta = dta + timeout_read(conn, my_look_len-sizeof(dta), 5);
        do {
          newdta = conn->read(my_look_len-sizeof(dta));
        } while (!newdta && conn->is_open());
        DEBUG(5, "Read from conn: %O\n", newdta);
      }
      dta = dta + newdta;
      if(sizeof(dta) < my_look_len)
       {
         if(!keep)
           set_network_mode(MODE_NONBLOCK);
         set_conn_callbacks_nonblocking();
         error("server shorted us!\n");
       }
      else if(sizeof(dta) > my_look_len)
      {
        read_buffer += dta[my_look_len..];
set_conn_callbacks_nonblocking();
//        backend->call_out(read_loop, 0);
      }

       Packet.PMQPacket p = parse_packet(dta[0..my_look_len-1], 1);
     if(!keep)
       set_network_mode(MODE_NONBLOCK);

       if(!p) error("couldn't parse the packet!\n");      
       set_conn_callbacks_nonblocking();

       return p;
   
    }
    else DEBUG(1, "no conn!\n");
  }

  void destroy()
  {
    conn->close();
    conn->set_read_callback(0);
    conn->set_close_callback(0);
    conn = 0;
    backend = 0;
    DEBUG(4, "PMQConnection: destroy!\n");
  }
