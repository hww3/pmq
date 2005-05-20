  inherit .PMQPacket;
  string type = "POSTMESSAGE";

  string encoded = "";
  Message.PMQMessage _message;
  string message_body;
  mapping headers;

  void create()
  {
    msg_data->hostname = gethostname();
    msg_data->localtime = time();
  }

  int get_size()
  {
    msg_data->message_type = _message->_typeof();
write(msg_data->message_type + "\n");
write((string)_message);
    msg_data->message = (string)_message;
    encoded = encode_value(msg_data);
    return sizeof(encoded);
  }

  string get_data()
  {
    return encoded;
  }

  void set_pmqmessage(Message.PMQMessage m)
  {
    _message = m;
  }

  Message.PMQMessage get_pmqmessage()
  {
    return _message;
  }

  void set_queue(string queue)
  {
    this->msg_data->queue = queue;
  }

  string get_queue()
  {
    return this->msg_data->queue;
  }

  string get_session()
  {
    return this->msg_data->session;
  }

  void set_session(string session)
  {
    this->msg_data->session = session;
  }

  void parse(string payload)
  {
    ::parse(payload);
write("decoding message of type " + msg_data->message_type + "\n");
    program p = master()->resolv("Message." + msg_data->message_type);
    
    if(!p)
      error("Unable to decode message of type " + msg_data->message_type + "\n");

    _message = p();
    _message->parse(MIME.Message(msg_data->message));
  }
  
