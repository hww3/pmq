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

  ADT.Queue net_queue = ADT.Queue();
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

  void run_backend()
  {
    do
    {
      float r = backend(5.0);
    } while(this);
  }

  void create(Stdio.File conn, PMQProperties config, mapping packets)
  {
    this->conn = conn;
    this->config = config;
    this->packets = packets;
    network_state = NETWORK_STATE_LOOKSTART;
      backend = Pike.Backend();
      handler = Thread.Thread(run_backend);

    if(get_mode() == MODE_CLIENT)
    {
//      DEBUG(2, "starting client backend thread\n");
      backend->call_out(conn->set_blocking, 0);
    }
    else 
    {
      set_conn_callbacks_nonblocking();
    }

  }

  void set_conn_callbacks_nonblocking()
  {
    conn->set_backend(backend);
//    backend->call_out(conn->set_backend, 0, backend);
    this->conn->set_nonblocking(remote_read, UNDEFINED, remote_close);
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
    int n, my_look_len;
    string packet_data="";
    read_buffer+=data;

//    DEBUG(5, "%O->remote_read(%O, %O)\n", this, id, data);

    if(network_state == NETWORK_STATE_LOOKCOMPLETE)
    {

      if(sizeof(read_buffer) < look_len) return;

      if(sizeof(read_buffer) == look_len)
      {
         packet_data = read_buffer;
         read_buffer = "";
      }

      else if(sizeof(read_buffer) > look_len)
      {
        packet_data = read_buffer[0.. (look_len-1)];
        read_buffer = read_buffer[look_len ..];
      }

      if(sizeof(packet_data)) parse_packet(packet_data, 0);
      if(!this) return;
      network_state = NETWORK_STATE_LOOKSTART;
      look_len = 0;
      
      if(sizeof(read_buffer))                
        backend->call_out(remote_read, 0, id, "");
      return;
    }

    else if(network_state == NETWORK_STATE_LOOKSTART)
    {
      if(sizeof(read_buffer) < 7) return; // we must have at least 7 bytes

      // PMQ plus payload len must be at the beginning of our buffer.
      n = sscanf(read_buffer, "PMQ%4c%s", my_look_len, read_buffer);

      if(n == 0) // we have a protocol error.
      {
	handle_protocol_error();
      }

      else  // we have a valid packet header.
      {
        look_len = my_look_len;
        packet_data = "";
        network_state = NETWORK_STATE_LOOKCOMPLETE;
        remote_read(id, "");
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

    if(catch(packet->parse(packet_payload)))
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

  void handle_packet(Packet.PMQPacket packet)
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
      net_queue->write(packet);
    else if(this->conn)
    {
      DEBUG(4, sprintf("%O->send_packet(%O)\n", this, packet));
      conn->write((string)packet);
    }
    else DEBUG(1, "no conn!\n");
  }


  void set_network_mode(int mode)
  {
    if(mode == MODE_NONBLOCK)
    {
      net_mode = MODE_NONBLOCK;
      if(!net_queue->is_empty())
      {
        do
        {
DEBUG(2, "catching up with queued packets.\n");
          send_packet(net_queue->read());
        }
        while(!net_queue->is_empty());
      }

    }

    else net_mode = MODE_BLOCK;
  }

  Packet.PMQPacket send_packet_await_response(Packet.PMQPacket packet)
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
      conn->set_blocking();
      DEBUG(4, sprintf("%O->send_packet_await_response(%O)\n", this, packet));
      send_packet(packet, 1);
      dta = conn->read(7);
//      dta = timeout_read(conn, 7, 5);
DEBUG(5, "Read from conn: %O\n", dta);
      if(!dta || sizeof(dta) < 7)
      {
        DEBUG(2, "unexpected response from remote: %O.\n", dta);
     set_network_mode(MODE_NONBLOCK);
        return 0;
      }
      // PMQ plus payload len must be at the beginning of our buffer.
      n = sscanf(dta, "PMQ%4c%s", my_look_len, dta);
   
      if(!n) error("unexpected response from server.\n");
      if(sizeof(dta) < my_look_len)
      {
//        dta = dta + timeout_read(conn, my_look_len-sizeof(dta), 5);
        dta = dta + conn->read(my_look_len-sizeof(dta));
        DEBUG(5, "Read from conn: %O\n", dta);
      }
      if(sizeof(dta) < my_look_len)
       {
     set_network_mode(MODE_NONBLOCK);
        error("server shorted us!\n");
       }
      else if(sizeof(dta) > my_look_len)
      {
        backend->call_out(remote_read, 0, "", dta[my_look_len..]);
      }

       Packet.PMQPacket p = parse_packet(dta, 1);
     set_network_mode(MODE_NONBLOCK);

       if(!p) error("couldn't parse the packet!\n");      
       set_conn_callbacks_nonblocking();

       return p;
   
    }
    else DEBUG(1, "no conn!\n");
  }

  void destroy()
  {
write("destory");
    conn->close();
    conn->set_read_callback(0);
    DEBUG(4, "PMQConnection: destroy!\n");
  }
