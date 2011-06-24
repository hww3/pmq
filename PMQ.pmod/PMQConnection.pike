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
  string read_buffer = "";
  int network_state = NETWORK_STATE_START;
  int connection_state = CONNECTION_START;
  int stop_backend = 0;
  int look_len = 0;
  int packet_num = 0;
  int last_error = 0;
  string client_id = 0;
  string protocol_version = 0;
  int block_read_timeout = 10;

  mapping packets;
  mapping incoming_packets = ([]);
  mapping incoming_waiters = ([]);

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
          werror(master()->describe_error(e));
        }

    } while(this && conn->is_open() && !stop_backend);
  }

  void create(Stdio.File conn, PMQProperties config, mapping packets, void|Pike.Backend b)
  {
    this->conn = conn;
    this->config = config;
    this->packets = packets;
    network_state = NETWORK_STATE_LOOKSTART;

    if(b)
    {
      backend = b;
    }
    else
    {
      if(get_mode() == MODE_CLIENT)
      {
        backend = Pike.Backend();
      }
      else backend = Pike.DefaultBackend;
    }

    if(get_mode() == MODE_CLIENT)
      handler = Thread.Thread(run_backend);

     set_conn_callbacks_nonblocking();
  }

  void set_conn_callbacks_nonblocking()
  {
    conn->set_backend(backend);
    backend->call_out(conn->set_nonblocking, 0 , remote_read, UNDEFINED, 
remote_close);
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
//write("remote close\n");
    connection_state = CONNECTION_DISCONNECT;
    DEBUG(4, "remote close\n");
    destruct();    
  }

  void remote_read(string id, string data)
  {
    read_buffer+=data;

    DEBUG(5, "%O->remote_read(%O, %O)\n", this, id, data);
    read_loop();
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

      if(sizeof(packet_data)) parse_packet(packet_data);
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

  Packet.PMQPacket collect_packet(string reply_id, int|void timeout, int|void internal)
  {
    Packet.PMQPacket p;
//werror("waiting for a packet with reply_id = %O\n", reply_id);
    if(incoming_packets[reply_id])
    {
      p = incoming_packets[reply_id];
      m_delete(incoming_packets, reply_id);
      return p;
    }       
    else if(!internal)// we must wait for the packet to arrive.
    {
      float res;
      Pike.Backend b = Pike.Backend();
      incoming_waiters[reply_id] = b; 
      res = b(5.0);
      m_delete(incoming_waiters, reply_id);
      destruct(b);
      return collect_packet(reply_id, UNDEFINED, 1);
    }

werror("collect_packet got nothing.\n");
    return 0;
  }

  void parse_packet(string packet_data)
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
//werror("have packet: %O\n", packet);
    backend->call_out(handle_packet, 0, packet);

    return;
  }

  void waiter_set_packet(Packet.PMQPacket p)
  {
//werror("waiter_set_packet called for %O\n", p->get_reply_id());
    incoming_packets[p->get_reply_id()] = p;
    return 0;
  }

void|int handle_packet(Packet.PMQPacket packet)
{
    DEBUG(4, sprintf("%O->handle_packet(%O)\n", this, packet));
//werror("have packet: %O %O\n", packet, packet->get_reply_id());
    if(packet->get_reply_id())
    {
      if(incoming_waiters[packet->get_reply_id()]) 
      {
        incoming_waiters[packet->get_reply_id()]
           ->call_out(waiter_set_packet, 0, packet);
        return 1;
      }
      else
      {
        if(incoming_packets[packet->get_reply_id()])
          werror("packet with duplicate id already in waiting queue.\n");
        waiter_set_packet(packet);
        return 1;
      }
    }
    else
     return 0;
  }

  void send_packet(Packet.PMQPacket packet)
  {

DEBUG(1, "%O->send_packet(%O)\n", this, packet);
    if(!conn->is_open())
    {
      DEBUG(3, "closing conn\n");
      conn->close();
    }
      int written = 0;
      string pkt = (string)packet;
      int towrite = strlen(pkt);

      DEBUG(4, sprintf("%O->send_packet(%O)\n", this, packet));

      do
       written+=conn->write(pkt);
     while(written<towrite);
  return;
  }

  Packet.PMQPacket send_packet_await_response(Packet.PMQPacket packet, 
          int|void keep)
  {
    if(!conn->is_open())
    {
      conn->close();
    }

    if(this->conn)
    {
      DEBUG(4, sprintf("%O->send_packet_await_response(%O)\n", this, packet));

      packet->set_id("c" + (string)random(10e+5));

      if(catch(send_packet(packet)))
         error("error while sending a packet.\n");

      Packet.PMQPacket p = collect_packet(packet->get_id(), 10);
 
      if(!p) error("couldn't collect a packet!\n");      

       return p;
   
    }
    else DEBUG(1, "no conn!\n");
  }

  void destroy()
  {
    stop_backend = 1;
    backend->call_out(lambda(){ return; }, 0);
    conn->set_read_callback(0);
    conn->set_close_callback(0);
    conn->close();
    destruct(conn);
//    destruct(backend);
    conn = 0;
    backend = 0;
    DEBUG(4, "PMQConnection: destroy!\n");
  }
