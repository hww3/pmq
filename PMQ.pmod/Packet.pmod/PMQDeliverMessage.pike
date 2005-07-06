  import PMQ; inherit .PMQPacket;
  string type = "DELIVERMESSAGE";

  string encoded = "";
  mapping headers;
  Message.PMQMessage _message;
  string message_body = "";

  void create()
  {
    msg_data->hostname = gethostname();
    msg_data->localtime = time();
  }

  int get_size()
  {
    msg_data->message_type = _message->_typeof();
    msg_data->message = (string)_message;
    encoded = encode_value(msg_data);
    return sizeof(encoded);
  }

  string get_data()
  {
    return encoded;
  }

  mapping get_headers()
  {
    return this->headers;
  }

  int get_ack()
  {
    return this->msg_data->ack;
  }
 
  void set_ack(int flag)
  {
    this->msg_data->ack = flag;
  }

  object get_pmqmessage()
  {
    return _message;
  }

  void set_pmqmessage(Message.PMQMessage m)
  {
    _message = m;
  }

  void set_session(string session)
  {
    this->msg_data->session = session;
  }

  string get_session()
  {
    return this->msg_data->session;
  }

  void parse(string payload)
  {
    ::parse(payload);
    program p = master()->resolv("PMQ.Message." + msg_data->message_type);

    if(!p)
      error("Unable to decode message of type " + msg_data->message_type + "\n");

    _message = p();
    _message->parse(MIME.Message(msg_data->message));
  }
